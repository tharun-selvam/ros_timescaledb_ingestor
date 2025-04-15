#!/usr/bin/env python3

import rospy
import psycopg2
import psycopg2.extras
import threading
from datetime import datetime
import importlib
import json
from queue import Queue, Full, Empty
import time # Import the time module
import cProfile, pstats # Add near other imports
import io # Add near other imports

# --- Globals ---
DB_HOST = ""
DB_PORT = 5432
DB_NAME = ""
DB_USER = ""
DB_PASSWORD = ""
TOPIC_NAME = ""
TOPIC_MSG_TYPE_STR = ""
DB_TABLE_NAME = ""
COLUMN_MAPPING = {}
BATCH_SIZE = 500 # Default batch size adjusted
FLUSH_INTERVAL_SECONDS = 5.0
# DB state & Threading
connection = None
cursor = None
MsgType = None
db_write_queue = Queue(maxsize=10)
db_worker_thread = None
stop_db_worker = threading.Event()
# Local buffer for message callback
current_batch = []
batch_lock = threading.Lock()
# Statistics
total_inserted_count = 0
counter_lock = threading.Lock()

profiler = cProfile.Profile() # Global profiler instance
profiled_message_count = 0 # Counter
MAX_PROFILE_MESSAGES = 200 # Profile only N messages to limit file size

def get_nested_attr(obj, attr_path):
    """Safely retrieves nested message attributes using dot notation."""
    attrs = attr_path.split('.')
    try:
        for attr in attrs:
            if '[' in attr and attr.endswith(']'):
                 base_attr, index_str = attr.split('[')
                 index = int(index_str[:-1])
                 obj = getattr(obj, base_attr)[index]
            else:
                obj = getattr(obj, attr)
        return obj
    except (AttributeError, IndexError, TypeError) as e:
        rospy.logwarn_throttle(10, f"Could not retrieve attribute '{attr_path}': {e}")
        return None

def connect_db():
    """Establishes connection to the TimescaleDB database, retrying on failure."""
    global connection, cursor
    if cursor: cursor.close()
    if connection: connection.close()
    connection = None
    cursor = None

    while not rospy.is_shutdown() and connection is None:
        try:
            rospy.loginfo(f"DB Writer: Connecting to database '{DB_NAME}' at {DB_HOST}:{DB_PORT}...")
            connection = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            cursor = connection.cursor()
            rospy.loginfo("DB Writer: Database connection successful.")
        except psycopg2.OperationalError as e:
            rospy.logerr(f"DB Writer: Database connection failed: {e}. Retrying in 5 seconds...")
            connection = None
            cursor = None
            rospy.sleep(5.0)

# --- DB Worker Thread Function (with timing and queue size logging) ---
def database_writer_thread():
    global connection, cursor, total_inserted_count
    rospy.loginfo("DB Writer thread started.")
    connect_db()

    while not stop_db_worker.is_set():
        try:
            batch_to_write = db_write_queue.get(block=True, timeout=1.0)

            if connection is None or connection.closed != 0:
                rospy.logwarn("DB Writer: Connection lost. Attempting reconnect.")
                connect_db()
                if connection is None:
                    rospy.logerr("DB Writer: Reconnect failed. Dropping batch.")
                    db_write_queue.task_done()
                    continue

            try:
                columns = list(COLUMN_MAPPING.keys())
                num_columns = len(columns)
                placeholders = ', '.join(['%s'] * num_columns)
                sql = f"INSERT INTO {DB_TABLE_NAME} ({', '.join(columns)}) VALUES ({placeholders})"
                batch_size = len(batch_to_write)

                # --- Timing Start ---
                start_time = time.monotonic()

                psycopg2.extras.execute_batch(cursor, sql, batch_to_write)
                connection.commit()

                # --- Timing End ---
                end_time = time.monotonic()
                duration = end_time - start_time

                # --- Increment and Log Count, Timing, Queue Size ---
                with counter_lock:
                    total_inserted_count += batch_size
                    q_size = db_write_queue.qsize() # Get current queue size
                    rospy.loginfo(f"DB Writer: Batch committed ({batch_size} rows, took {duration:.4f}s). Total: {total_inserted_count}. Queue size: {q_size}")
                # --- End Logging ---

            except psycopg2.Error as e:
                rospy.logerr(f"DB Writer: Insert/commit error: {e}")
                if connection:
                    try: connection.rollback()
                    except psycopg2.Error as rb_e: rospy.logerr(f"DB Writer: Rollback failed: {rb_e}")
            except Exception as e:
                 rospy.logerr(f"DB Writer: Unexpected processing error: {e}")

            db_write_queue.task_done()

        except Empty:
            # Log queue size even on timeout if it's non-zero
            q_size_on_timeout = db_write_queue.qsize()
            if q_size_on_timeout > 0:
                rospy.logwarn(f"DB Writer: Queue timeout. Current queue size: {q_size_on_timeout}")
            continue # Timeout occurred, check stop signal and loop
        except Exception as e:
             rospy.logerr(f"DB Writer: Error in main loop: {e}")
             rospy.sleep(0.5)

    if cursor: cursor.close()
    if connection: connection.close()
    rospy.loginfo("DB Writer thread finished and connection closed.")


def flush_current_batch():
    """Copies the current batch and puts it on the queue for writing."""
    global current_batch
    with batch_lock:
        if not current_batch:
            return
        batch_copy = current_batch[:]
        current_batch = []

    if batch_copy:
        try:
            # Put the copied batch onto the queue for the writer thread
            db_write_queue.put(batch_copy, block=True, timeout=0.5)
            rospy.logdebug(f"Placed batch of {len(batch_copy)} onto DB write queue (current qsize: {db_write_queue.qsize()}).")
        except Full:
            rospy.logwarn(f"DB write queue is full! Dropping batch of {len(batch_copy)}.")
        except Exception as e:
            rospy.logerr(f"Error putting batch onto queue: {e}")


def timer_callback(event):
    """Periodically triggers flushing the current batch."""
    # Log queue size from timer perspective too, less frequently
    # rospy.loginfo_throttle(10.0, f"Timer Check: DB write queue size: {db_write_queue.qsize()}")
    if len(current_batch) > 0: # Avoid locking if batch is empty
        flush_current_batch()


def message_callback(msg):
    """Processes incoming ROS messages and adds them to the local batch buffer."""
    global current_batch, profiled_message_count
    # --- Profiling Start ---
    do_profile = False
    if profiled_message_count < MAX_PROFILE_MESSAGES:
        profiler.enable()
        do_profile = True
        profiled_message_count += 1
    # --- Profiling End ---
    row_data = []
    valid_row = True

    for db_col, ros_field in COLUMN_MAPPING.items():
        value = get_nested_attr(msg, ros_field)

        if hasattr(value, 'secs') and hasattr(value, 'nsecs'): # Check for ROS Time
            if value.is_zero(): value = None
            else:
                try: value = datetime.utcfromtimestamp(value.to_sec())
                except ValueError as e:
                    rospy.logwarn(f"Timestamp conversion error: {e}. Storing NULL.")
                    value = None

        elif ros_field in ["ranges", "intensities"] and isinstance(value, (list, tuple)): # Handle JSONB arrays
            try: value = json.dumps(value)
            except TypeError as e:
                 rospy.logwarn(f"JSON conversion error for field '{ros_field}': {e}. Storing NULL.")
                 value = None

        if value is None and ros_field in ['header.stamp', 'time']: # Check for critical None timestamp
             rospy.logwarn_throttle(5, f"Timestamp field '{ros_field}' is None. Skipping message.")
             valid_row = False
             break

        row_data.append(value)

    # --- Profiling Stop ---
    if do_profile:
        profiler.disable()
    # --- Profiling End ---

    if valid_row:
        with batch_lock:
            current_batch.append(tuple(row_data))
            if len(current_batch) >= BATCH_SIZE:
                flush_current_batch() # Flush when batch size is reached


def import_message_type(type_str):
    """Dynamically imports the ROS message class from a 'package/MsgName' string."""
    try:
        pkg_name, msg_name = type_str.split('/')
        module = importlib.import_module(pkg_name + '.msg')
        return getattr(module, msg_name)
    except (ValueError, ImportError, AttributeError) as e:
        rospy.logfatal(f"Failed to import message type '{type_str}': {e}")
        rospy.signal_shutdown(f"Invalid message type: {type_str}")
        return None


if __name__ == '__main__':
    rospy.init_node('timescaledb_ingestor_node', anonymous=True)

    # --- Load Parameters ---
    try:
        DB_HOST = rospy.get_param("~db_host")
        DB_PORT = rospy.get_param("~db_port", 5432)
        DB_NAME = rospy.get_param("~db_name")
        DB_USER = rospy.get_param("~db_user")
        DB_PASSWORD = rospy.get_param("~db_password")
        TOPIC_NAME = rospy.get_param("~topic_name")
        TOPIC_MSG_TYPE_STR = rospy.get_param("~topic_msg_type")
        DB_TABLE_NAME = rospy.get_param("~db_table_name")
        COLUMN_MAPPING = rospy.get_param("~column_mapping")
        BATCH_SIZE = rospy.get_param("~batch_size", 100) # Default adjusted
        FLUSH_INTERVAL_SECONDS = rospy.get_param("~flush_interval", 5.0)

        if not isinstance(COLUMN_MAPPING, dict) or not COLUMN_MAPPING:
             raise ValueError("'column_mapping' parameter must be a non-empty dictionary.")

    except KeyError as e:
        rospy.logfatal(f"Required parameter missing: {e}. Shutting down.")
        exit(1)
    except ValueError as e:
         rospy.logfatal(f"Parameter error: {e}. Shutting down.")
         exit(1)

    # --- Import Message Type ---
    MsgType = import_message_type(TOPIC_MSG_TYPE_STR)
    if MsgType is None: exit(1)

    # --- Start DB Worker Thread ---
    stop_db_worker.clear()
    db_worker_thread = threading.Thread(target=database_writer_thread, daemon=True)
    db_worker_thread.start()

    # --- Register Shutdown Hook ---
    def shutdown_hook_main():
        rospy.loginfo("Main Shutdown: Signaling DB worker to stop...")
        stop_db_worker.set()
        flush_current_batch() # Attempt to queue final batch
        rospy.loginfo("Main Shutdown: Waiting for DB queue to empty (max 5 sec)...")
        # Wait briefly for queue processing, but don't block indefinitely
        # Note: queue.join() could block forever if worker thread died unexpectedly
        # A timeout approach might be safer in complex scenarios
        time.sleep(0.1) # Allow a moment for last item to be potentially queued
        q_wait_start = time.monotonic()
        while not db_write_queue.empty() and (time.monotonic() - q_wait_start < 5.0):
             time.sleep(0.1)
        if not db_write_queue.empty():
             rospy.logwarn(f"Main Shutdown: DB queue still has {db_write_queue.qsize()} items after waiting.")

        rospy.loginfo("Main Shutdown: Waiting for DB worker thread to finish...")
        if db_worker_thread:
             db_worker_thread.join(timeout=5.0) # Wait max 5 sec for thread

        # --- Dump Profiling Stats ---
        if profiled_message_count > 0:
            stats_file = "/tmp/ingestor_profile.prof" # Or choose another path if needed
            rospy.loginfo(f"Main Shutdown: Dumping profile stats for {profiled_message_count} messages to {stats_file}")
            profiler.dump_stats(stats_file)
        # --- End Dump Stats ---

        rospy.loginfo(f"Main Shutdown: Complete. Final total inserted count: {total_inserted_count}")

    rospy.on_shutdown(shutdown_hook_main)

    # --- Setup Subscriber and Timer ---
    rospy.Subscriber(TOPIC_NAME, MsgType, message_callback, queue_size=BATCH_SIZE*2)
    rospy.Timer(rospy.Duration(FLUSH_INTERVAL_SECONDS), timer_callback)

    rospy.loginfo(f"TimescaleDB Ingestor node started for topic '{TOPIC_NAME}'.")
    rospy.loginfo(f"Ingesting to table '{DB_TABLE_NAME}'. Batch size: {BATCH_SIZE}, Flush interval: {FLUSH_INTERVAL_SECONDS}s.")

    rospy.spin()
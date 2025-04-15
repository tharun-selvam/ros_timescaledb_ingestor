#!/usr/bin/env python3

import rospy
import psycopg2
import psycopg2.extras # For execute_batch
import threading
from datetime import datetime
import importlib
import json

# --- Globals ---
# DB Connection details (Loaded from ROS Params)
DB_HOST = ""
DB_PORT = 5432
DB_NAME = ""
DB_USER = ""
DB_PASSWORD = ""
# Topic details (Loaded from ROS Params)
TOPIC_NAME = ""
TOPIC_MSG_TYPE_STR = ""
DB_TABLE_NAME = ""
COLUMN_MAPPING = {}
# Batching config (Loaded from ROS Params)
BATCH_SIZE = 10000
FLUSH_INTERVAL_SECONDS = 5.0
# DB state
connection = None
cursor = None
data_buffer = []
buffer_lock = threading.Lock()
MsgType = None # Imported message class

def get_nested_attr(obj, attr_path):
    """Safely retrieves nested message attributes using dot notation."""
    attrs = attr_path.split('.')
    try:
        for attr in attrs:
            if '[' in attr and attr.endswith(']'): # Handle list indices like ranges[0]
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
    while not rospy.is_shutdown() and connection is None:
        try:
            rospy.loginfo(f"Connecting to database '{DB_NAME}' at {DB_HOST}:{DB_PORT}...")
            connection = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            cursor = connection.cursor()
            rospy.loginfo("Database connection successful.")
        except psycopg2.OperationalError as e:
            rospy.logerr(f"Database connection failed: {e}. Retrying in 5 seconds...")
            rospy.sleep(5.0)

def shutdown_hook():
    """Flushes final buffer and closes DB connection on node shutdown."""
    rospy.loginfo("Shutdown signal received. Flushing final buffer...")
    flush_buffer()
    if cursor:
        cursor.close()
        rospy.loginfo("Database cursor closed.")
    if connection:
        connection.close()
        rospy.loginfo("Database connection closed.")

def flush_buffer():
    """Inserts the buffered data into the database using execute_batch."""
    global data_buffer
    with buffer_lock:
        if not data_buffer:
            return
        buffer_copy = data_buffer[:]
        data_buffer = []

    if not buffer_copy:
        return

    try:
        columns = list(COLUMN_MAPPING.keys())
        num_columns = len(columns)
        num_values_in_first_row = len(buffer_copy[0])

        # Sanity check (shouldn't fail with current logic, but good practice)
        if num_columns != num_values_in_first_row:
            rospy.logerr(f"CRITICAL: Column count ({num_columns}) != value count ({num_values_in_first_row}). Batch dropped.")
            rospy.logerr(f"Columns: {columns}")
            rospy.logerr(f"First Row Values: {buffer_copy[0]}")
            return

        # Create '%s, %s, ..., %s' string for placeholders
        placeholders = ', '.join(['%s'] * num_columns)
        sql = f"INSERT INTO {DB_TABLE_NAME} ({', '.join(columns)}) VALUES ({placeholders})"

        # Use execute_batch for efficient bulk insertion
        psycopg2.extras.execute_batch(cursor, sql, buffer_copy)
        connection.commit()
        rospy.logdebug(f"Successfully inserted batch of {len(buffer_copy)}.")

    except psycopg2.Error as e:
        rospy.logerr(f"Database insert/commit error: {e}")
        rospy.logerr(f"Failed data batch sample (first item): {buffer_copy[0] if buffer_copy else 'N/A'}")
        if connection:
            try:
                connection.rollback() # Rollback transaction on error
            except psycopg2.Error as rb_e:
                 rospy.logerr(f"Rollback failed: {rb_e}")
    except Exception as e:
        rospy.logerr(f"An unexpected error occurred during flush: {e}")
        rospy.logerr(f"Failed data batch sample (first item): {buffer_copy[0] if buffer_copy else 'N/A'}")
        rospy.logerr(f"SQL attempted: {sql if 'sql' in locals() else 'SQL not generated'}")

def timer_callback(event):
    """Periodically checks DB connection and flushes the buffer."""
    if connection is None or connection.closed != 0:
        rospy.logwarn("Database connection lost. Attempting to reconnect...")
        connect_db()
        if connection is None: # If still not connected, skip flush
             return

    if len(data_buffer) > 0:
        flush_buffer()

def message_callback(msg):
    """Processes incoming ROS messages and adds them to the buffer."""
    row_data = []
    valid_row = True

    for db_col, ros_field in COLUMN_MAPPING.items():
        value = get_nested_attr(msg, ros_field)

        # Convert ROS Time to Python datetime
        if hasattr(value, 'secs') and hasattr(value, 'nsecs'):
            if value.is_zero():
                value = None
            else:
                try:
                    value = datetime.utcfromtimestamp(value.to_sec())
                except ValueError as e:
                    rospy.logwarn(f"Could not convert timestamp {value.to_sec()} for field '{ros_field}': {e}. Storing NULL.")
                    value = None

        # Convert lists/tuples to JSON strings for JSONB columns
        elif ros_field in ["ranges", "intensities"] and isinstance(value, (list, tuple)):
            try:
                value = json.dumps(value)
            except TypeError as e:
                 rospy.logwarn(f"Could not convert field '{ros_field}' to JSON: {e}. Storing NULL.")
                 value = None

        # Check if critical timestamp became None AFTER potential conversion attempt
        if value is None and ros_field in ['header.stamp', 'time']:
             rospy.logwarn_throttle(5, f"Timestamp field '{ros_field}' resolved to None. Skipping message.")
             valid_row = False
             break

        row_data.append(value)

    if valid_row:
        with buffer_lock:
            data_buffer.append(tuple(row_data))
            # Trigger early flush only if buffer grows excessively large
            if len(data_buffer) >= BATCH_SIZE * 1.5:
                 flush_buffer()


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
        BATCH_SIZE = rospy.get_param("~batch_size", 100)
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
    if MsgType is None:
         exit(1)

    # --- Connect to DB ---
    connect_db()
    if connection is None:
         rospy.logfatal("Failed to connect to database on startup. Shutting down.")
         exit(1)

    # --- Register Shutdown Hook ---
    rospy.on_shutdown(shutdown_hook)

    # --- Setup Subscriber and Timer ---
    rospy.Subscriber(TOPIC_NAME, MsgType, message_callback, queue_size=BATCH_SIZE*2)
    rospy.Timer(rospy.Duration(FLUSH_INTERVAL_SECONDS), timer_callback)

    rospy.loginfo(f"TimescaleDB Ingestor node started for topic '{TOPIC_NAME}'.")
    rospy.loginfo(f"Ingesting to table '{DB_TABLE_NAME}'. Batch size: {BATCH_SIZE}, Flush interval: {FLUSH_INTERVAL_SECONDS}s.")

    rospy.spin() # Keep node alive
#!/usr/bin/env python3

import rospy
import rosbag
import psycopg2
import threading
from datetime import datetime
import importlib
import json
import os
import time
import signal
import io

# --- Globals ---
DB_HOST = ""
DB_PORT = 5432
DB_NAME = ""
DB_USER = ""
DB_PASSWORD = ""
DB_TABLE_NAME = ""
COLUMN_MAPPING = {} # Mapping from DB Col -> ROS Msg Field
BAG_DIRECTORY = "" # Directory to watch for completed bags
POLL_INTERVAL_SEC = 2.0 # How often to check for new bags
TOPIC_NAME_IN_BAG = "" # The topic name as recorded in the bag
# DB state
connection = None
cursor = None
stop_flag = threading.Event()
# Statistics
total_processed_bags = 0
total_inserted_rows = 0
processed_files = set() # Keep track of files already processed in this run


def get_nested_attr(obj, attr_path):
    """Safely retrieves nested message attributes using dot notation."""
    attrs = attr_path.split('.')

    for attr in attrs:
        if '[' in attr and attr.endswith(']'): # Handle list indices like ranges[0]
                base_attr, index_str = attr.split('[')
                index = int(index_str[:-1])
                obj = getattr(obj, base_attr)[index]
        else:
            obj = getattr(obj, attr)
    return obj
    

def connect_db():
    """Establishes connection to the TimescaleDB database."""
    global connection, cursor
    # Close existing if trying to reconnect
    if cursor: cursor.close()
    if connection: connection.close()
    connection = None
    cursor = None

    while not stop_flag.is_set() and connection is None:
        try:
            rospy.loginfo(f"Processor: Connecting to database '{DB_NAME}' at {DB_HOST}:{DB_PORT}...")
            connection = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            cursor = connection.cursor()
            rospy.loginfo("Processor: Database connection successful.")
        except psycopg2.OperationalError as e:
            rospy.logerr(f"Processor: Database connection failed: {e}. Retrying in 5 seconds...")
            connection = None
            cursor = None
            time.sleep(5.0)

def format_copy_data(row_tuple):
    """Formats a tuple of data into a tab-delimited string for COPY, handling None and escaping."""
    line_parts = []
    for item in row_tuple:
        if item is None:
            line_parts.append("\\N") # NULL for COPY
        elif isinstance(item, str):
            # Basic escaping for COPY: replace \ with \\, \t with \t, \n with \n
            escaped = item.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n')
            line_parts.append(escaped)
        elif isinstance(item, datetime):
             # Format to ISO 8601 style suitable for TIMESTAMPTZ
             line_parts.append(item.strftime('%Y-%m-%d %H:%M:%S.%f+00'))
        else:
            line_parts.append(str(item)) # Convert other types (float, int) to string
    return "\t".join(line_parts) + "\n"

def process_bag_file(bag_path):
    """Reads a bag file, converts messages, and inserts into DB using COPY."""
    global total_inserted_rows, total_processed_bags
    rospy.loginfo(f"Processor: Starting processing for bag: {bag_path}")
    rows_in_batch = 0
    batch_start_time = time.monotonic()

    # Ensure DB connection
    if connection is None or connection.closed != 0:
        connect_db()
        if connection is None:
            rospy.logerr("Processor: Cannot process bag, DB connection failed.")
            return False

    try:
        # Use StringIO to buffer data for copy_from
        copy_buffer = io.StringIO()
        cols_ordered = list(COLUMN_MAPPING.keys()) # Get column order

        with rosbag.Bag(bag_path, 'r') as bag:
            for topic, msg, t in bag.read_messages(topics=[TOPIC_NAME_IN_BAG]):
                # --- Data Conversion Logic (similar to real-time node) ---
                row_data_list = []
                valid_row = True
                for db_col in cols_ordered: # Iterate in fixed column order
                    ros_field = COLUMN_MAPPING[db_col]
                    value = get_nested_attr(msg, ros_field)

                    if hasattr(value, 'secs') and hasattr(value, 'nsecs'): # ROS Time
                        if value.is_zero(): value = None
                        else:
                            try: value = datetime.utcfromtimestamp(value.to_sec())
                            except ValueError: value = None
                    elif ros_field in ["ranges", "intensities"] and isinstance(value, (list, tuple)): # JSON
                        try: value = json.dumps(value)
                        except TypeError: value = None

                    if value is None and ros_field in ['header.stamp', 'time']: # Critical timestamp check
                        rospy.logwarn(f"Processor: Timestamp None in bag {os.path.basename(bag_path)}. Skipping message.")
                        valid_row = False
                        break # Skip this message

                    row_data_list.append(value)
                # --- End Data Conversion ---

                if valid_row:
                    if len(row_data_list) == len(cols_ordered):
                        copy_buffer.write(format_copy_data(tuple(row_data_list)))
                        rows_in_batch += 1
                    else:
                        rospy.logwarn(f"Processor: Mismatch between columns ({len(cols_ordered)}) and values ({len(row_data_list)}) for a message. Skipping.")


        # --- Perform COPY Operation ---
        copy_buffer.seek(0) # Rewind buffer to beginning
        sql_copy = f"COPY {DB_TABLE_NAME} ({', '.join(cols_ordered)}) FROM STDIN"
        rospy.loginfo(f"Processor: Executing COPY for {rows_in_batch} rows from {os.path.basename(bag_path)}...")
        try:
            start_copy_time = time.monotonic()
            cursor.copy_expert(sql=sql_copy, file=copy_buffer)
            connection.commit()
            end_copy_time = time.monotonic()
            duration = end_copy_time - start_copy_time
            total_inserted_rows += rows_in_batch
            total_processed_bags += 1
            rospy.loginfo(f"Processor: COPY successful ({rows_in_batch} rows, took {duration:.4f}s). Total Inserted: {total_inserted_rows}. Bags Processed: {total_processed_bags}")
            return True # Indicate success
        except psycopg2.Error as e:
            rospy.logerr(f"Processor: Database COPY/commit error: {e}")
            if connection:
                try: connection.rollback()
                except psycopg2.Error as rb_e: rospy.logerr(f"Processor: Rollback failed: {rb_e}")
            return False # Indicate failure
        except Exception as e:
            rospy.logerr(f"Processor: Unexpected error during COPY: {e}")
            if connection:
                try: connection.rollback()
                except psycopg2.Error as rb_e: rospy.logerr(f"Processor: Rollback failed: {rb_e}")
            return False # Indicate failure

    except rosbag.ROSBagException as e:
        rospy.logerr(f"Processor: Error reading bag file {bag_path}: {e}")
        return False # Indicate failure (bag might be corrupt)
    except Exception as e:
        rospy.logerr(f"Processor: Unexpected error processing bag {bag_path}: {e}")
        return False # Indicate failure


def main_processor_loop():
    """Periodically scans for completed bags and processes them."""
    rospy.loginfo("Starting Bag Processor Loop...")
    processed_files.clear() # Clear set on startup

    while not stop_flag.is_set():
        try:
            found_new_bag = False
            # Scan directory for completed bags (no .active suffix)
            for filename in os.listdir(BAG_DIRECTORY):
                if filename.endswith(".bag") and not filename.endswith(".active"):
                    full_path = os.path.join(BAG_DIRECTORY, filename)
                    if full_path not in processed_files:
                        found_new_bag = True
                        processed_files.add(full_path) # Add immediately to avoid race conditions

                        success = process_bag_file(full_path)

                        if success:
                            try:
                                os.remove(full_path)
                                rospy.loginfo(f"Processor: Deleted processed bag: {filename}")
                            except OSError as e:
                                rospy.logerr(f"Processor: Failed to delete processed bag {full_path}: {e}")
                                # Keep it in processed_files set so we don't retry immediately
                        else:
                            rospy.logerr(f"Processor: Failed to process bag {full_path}. It will remain in the directory and be skipped in this run.")
                            # Keep it in processed_files for this run, will retry next time node starts

            if not found_new_bag:
                rospy.logdebug("Processor: No new completed bags found.")

        except Exception as e:
             rospy.logerr(f"Processor: Error in main scan loop: {e}")

        # Wait before next scan
        stop_flag.wait(POLL_INTERVAL_SEC) # Wait for interval or stop signal

    rospy.loginfo("Bag Processor Loop finished.")
    # Close DB connection on exit
    if cursor: cursor.close()
    if connection: connection.close()

def shutdown_handler_proc(sig, frame):
    """Handle SIGINT/SIGTERM for clean shutdown."""
    rospy.loginfo("Processor: Shutdown signal received...")
    stop_flag.set()


if __name__ == '__main__':
    rospy.init_node('bag_processor_node', anonymous=True)

    # --- Load Parameters ---
    try:
        DB_HOST = rospy.get_param("~db_host") # Looks for /bag_processor/bag_processor_node/db_host
        DB_PORT = rospy.get_param("~db_port", 5432)
        DB_NAME = rospy.get_param("~db_name")
        DB_USER = rospy.get_param("~db_user")
        DB_PASSWORD = rospy.get_param("~db_password")
        DB_TABLE_NAME = rospy.get_param("~db_table_name")
        COLUMN_MAPPING = rospy.get_param("~column_mapping") # Looks for /bag_processor/bag_processor_node/column_mapping
        BAG_DIRECTORY = rospy.get_param("~bag_directory")
        POLL_INTERVAL_SEC = rospy.get_param("~poll_interval", 2.0)
        TOPIC_NAME_IN_BAG = rospy.get_param("~topic_name_in_bag")
        # ... rest of try block ...
    except KeyError as e:
        rospy.logfatal(f"Processor: Required parameter missing: {e}. Shutting down.")
        exit(1)
    except ValueError as e:
         rospy.logfatal(f"Processor: Parameter error: {e}. Shutting down.")
         exit(1)

    # --- Handle Shutdown Signal ---
    signal.signal(signal.SIGINT, shutdown_handler_proc)
    signal.signal(signal.SIGTERM, shutdown_handler_proc)

    # --- Run Main Loop ---
    main_processor_loop()

    rospy.loginfo("Bag processor node finished.")
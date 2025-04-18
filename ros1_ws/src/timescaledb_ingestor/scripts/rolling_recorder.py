#!/usr/bin/env python3

import rospy
import rosbag
from sensor_msgs.msg import LaserScan # Or your specific message type
import os
import uuid
import time
import signal
import threading
import importlib

# --- Globals ---
current_bag = None
current_bag_path = ""
bag_output_dir = ""
max_bag_duration_sec = 5.0 # How often to rotate bags
topic_name = ""
MsgType = None
stop_flag = threading.Event()
write_lock = threading.Lock() # Protect bag writing/closing

def import_message_type(type_str):
    """Dynamically imports the ROS message class from string."""
    try:
        pkg_name, msg_name = type_str.split('/')
        module = importlib.import_module(pkg_name + '.msg')
        return getattr(module, msg_name)
    except (ValueError, ImportError, AttributeError) as e:
        rospy.logfatal(f"Failed to import message type '{type_str}': {e}")
        return None

def open_new_bag():
    """Closes current bag (if open), renames it, and opens a new one."""
    global current_bag, current_bag_path
    with write_lock: # Ensure exclusive access during switch
        if current_bag:
            rospy.loginfo(f"Closing bag: {current_bag_path}")
            try:
                current_bag.close()
            except Exception as e:
                rospy.logerr(f"Error closing bag {current_bag_path}: {e}")

            # Rename to signal completion (remove .active)
            completed_bag_path = current_bag_path.replace(".active", "")
            try:
                os.rename(current_bag_path, completed_bag_path)
                rospy.loginfo(f"Bag completed and renamed to: {completed_bag_path}")
            except OSError as e:
                 rospy.logerr(f"Error renaming bag {current_bag_path} to {completed_bag_path}: {e}")
            current_bag = None
            current_bag_path = ""

        # Create new bag path
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        new_bag_name = f"ingest_{timestamp}_{unique_id}.bag.active"
        current_bag_path = os.path.join(bag_output_dir, new_bag_name)

        try:
            rospy.loginfo(f"Opening new bag: {current_bag_path}")
            current_bag = rosbag.Bag(current_bag_path, 'w')
        except Exception as e:
            rospy.logfatal(f"Failed to open new bag {current_bag_path}: {e}")
            current_bag = None # Ensure it's None on failure
            rospy.signal_shutdown("Failed to open bag")


def rotate_bag_callback(event):
    """Called by timer to rotate bags."""
    rospy.logdebug("Timer triggered bag rotation.")
    open_new_bag()

def message_callback(msg):
    """Writes incoming messages to the current bag file."""
    with write_lock: # Protect bag object access
        if current_bag:
            try:
                # Use message timestamp if available, otherwise ROS time
                ts = msg.header.stamp if hasattr(msg, 'header') and hasattr(msg.header, 'stamp') else rospy.Time.now()
                current_bag.write(topic_name, msg, t=ts)
            except Exception as e:
                rospy.logerr(f"Error writing message to bag {current_bag_path}: {e}")
        else:
             rospy.logwarn_throttle(10, "Current bag not open, message dropped.")


def shutdown_handler(sig, frame):
    """Handle SIGINT/SIGTERM for clean shutdown."""
    rospy.loginfo("Shutdown signal received...")
    stop_flag.set()
    # Close the final bag outside the lock maybe? Ensure timer isn't running.
    # Simpler: just close it.
    with write_lock:
        if current_bag:
            rospy.loginfo(f"Closing final bag: {current_bag_path}")
            try:
                current_bag.close()
            except Exception as e:
                 rospy.logerr(f"Error closing final bag {current_bag_path}: {e}")
            # Rename the final bag
            completed_bag_path = current_bag_path.replace(".active", "")
            try:
                os.rename(current_bag_path, completed_bag_path)
                rospy.loginfo(f"Final bag completed and renamed to: {completed_bag_path}")
            except OSError as e:
                rospy.logerr(f"Error renaming final bag {current_bag_path}: {e}")


if __name__ == '__main__':
    rospy.init_node('rolling_bag_recorder_node', anonymous=True)

    # --- Get Parameters ---
    topic_name = rospy.get_param("~topic_name", "/scan")
    topic_msg_type_str = rospy.get_param("~topic_msg_type", "sensor_msgs/LaserScan")
    bag_output_dir = rospy.get_param("~bag_directory", "/tmp/rosbags_for_ingest")
    max_bag_duration_sec = rospy.get_param("~max_bag_duration", 5.0)

    MsgType = import_message_type(topic_msg_type_str)
    if MsgType is None:
        exit(1)

    # --- Create output directory ---
    try:
        os.makedirs(bag_output_dir, exist_ok=True)
        rospy.loginfo(f"Using bag directory: {bag_output_dir}")
    except OSError as e:
        rospy.logfatal(f"Failed to create bag directory {bag_output_dir}: {e}")
        exit(1)

    # --- Setup ---
    open_new_bag() # Open the first bag
    if current_bag is None: # Check if initial opening failed
        exit(1)

    rospy.Subscriber(topic_name, MsgType, message_callback, queue_size=10000) # Adjust queue size as needed
    timer = rospy.Timer(rospy.Duration(max_bag_duration_sec), rotate_bag_callback)

    # --- Handle Shutdown Signal ---
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    rospy.loginfo(f"Rolling bag recorder started for topic '{topic_name}'. Rotating every {max_bag_duration_sec} seconds.")

    # Keep main thread alive while callbacks/timer run
    stop_flag.wait() # Wait until shutdown signal is received

    # Explicitly stop timer before final cleanup (though shutdown hook handles bag)
    timer.shutdown()
    rospy.loginfo("Rolling bag recorder node finished.")
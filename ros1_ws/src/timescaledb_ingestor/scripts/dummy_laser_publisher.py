#!/usr/bin/env python3

import rospy
from sensor_msgs.msg import LaserScan
import math
import random

count_msgs_sent = 0

def log():
    print(f"{count_msgs_sent} messages sent")

if __name__ == '__main__':
    rospy.init_node('dummy_laser_publisher_node', anonymous=True)

    # --- Get Parameters ---
    topic_name = rospy.get_param('~topic', '/scan') # Default topic name is /scan
    frame_id = rospy.get_param('~frame_id', 'laser_frame') # Default frame_id
    publish_rate_hz = rospy.get_param('~rate', 5.0) # Default publish rate 5 Hz

    # --- Create Publisher ---
    pub = rospy.Publisher(topic_name, LaserScan, queue_size=10)
    rate = rospy.Rate(publish_rate_hz)

    rospy.loginfo(f"Starting dummy LaserScan publisher on topic '{topic_name}' at {publish_rate_hz} Hz.")

    # --- LaserScan Message Configuration (Defaults) ---
    angle_min = -math.pi / 2.0 # -90 degrees
    angle_max = math.pi / 2.0 # +90 degrees
    angle_increment = math.pi / 180.0 # 1 degree increments
    time_increment = 0.0 # Assume instantaneous scan
    scan_time = 1.0 / publish_rate_hz # Time between scans
    range_min = 0.1 # Min sensor range
    range_max = 10.0 # Max sensor range

    # Calculate number of readings based on angular range and increment
    num_readings = int(math.ceil((angle_max - angle_min) / angle_increment)) + 1

    rospy.on_shutdown(log)

    # --- Main Publishing Loop ---
    sequence_id = 0
    while not rospy.is_shutdown():
        # Create LaserScan message
        scan_msg = LaserScan()

        # Populate Header
        scan_msg.header.seq = sequence_id
        scan_msg.header.stamp = rospy.Time.now()
        scan_msg.header.frame_id = frame_id

        # Populate scan parameters
        scan_msg.angle_min = angle_min
        scan_msg.angle_max = angle_max
        scan_msg.angle_increment = angle_increment
        scan_msg.time_increment = time_increment
        scan_msg.scan_time = scan_time
        scan_msg.range_min = range_min
        scan_msg.range_max = range_max

        # Populate ranges with dummy data (e.g., constant value with slight noise)
        dummy_ranges = []
        base_range = 2.5 # Base distance for dummy data
        for i in range(num_readings):
            # Add a little variation to make it slightly more interesting
            noise = random.uniform(-0.1, 0.1)
            # Ensure value is within min/max, though noise here is small
            range_val = max(range_min, min(range_max, base_range + noise))
            dummy_ranges.append(range_val)

        scan_msg.ranges = dummy_ranges
        scan_msg.intensities = [] # Leave intensities empty

        # Publish the message
        pub.publish(scan_msg)
        count_msgs_sent += 1
        rospy.logdebug(f"Published scan seq {sequence_id}") # Use logdebug to avoid spam

        sequence_id += 1
        rate.sleep() # Wait to maintain the desired publish rate

    rospy.loginfo("Dummy LaserScan publisher stopped.")
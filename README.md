## Log in to DB

```bash
psql -h <ip-address> -U robot_researcher -d robot_data
```
- password: `secret`
- databasename: `robot_data`

## Assumptions
- appropriate DB exists
- hypertable exists with the apporpriate columsn corresponding to the topic's message type
- an additional column exists for timestamp `TIMESTAMPZ`

## Creating database for laserscan msg type 
```sql
CREATE TABLE laser_scans (
    time TIMESTAMPTZ NOT NULL,

    frame_id TEXT, -- From header.frame_id
    angle_min REAL,
    angle_max REAL,
    angle_increment REAL,
    time_increment REAL,
    scan_time REAL,
    range_min REAL,
    range_max REAL,

    -- Array fields stored as JSONB
    ranges JSONB,
    intensities JSONB
);

COMMENT ON COLUMN laser_scans.time IS 'Timestamp from ROS message header (header.stamp)';
COMMENT ON COLUMN laser_scans.frame_id IS 'Frame ID from ROS message header (header.frame_id)';
COMMENT ON COLUMN laser_scans.ranges IS 'Laser scan range data [m], stored as a JSON array';
COMMENT ON COLUMN laser_scans.intensities IS 'Laser scan intensity data, stored as a JSON array';
```

- convert to hypertable
```sql
SELECT create_hypertable('laser_scans', 'time');
```

## TO DO 
- add a script to create the hypertable in the database 

**How to Use This for a Reusable Image:**

1.  **Build the Image:** Navigate to the directory containing the `Dockerfile` and run:
    ```bash
    docker build -t my-ros-timescaledb-env:latest .
    # Replace 'my-ros-timescaledb-env' with your desired image name/tag
    ```
2.  **Run a Container with Your Code Mounted:** Now, instead of using the complex `docker-compose.yml` or the base `ros:noetic-ros-base` directly, you run *your* custom image and mount only your `src` directory (or the `ros1_ws` directory if you prefer).
    ```bash
    # Example: Mount the whole workspace
    docker run -it --rm \
      -v "/Users/tharunselvam/Documents/my_ros_project/ros1_ws:/root/ros1_ws" \
      --name ros1_ingestor_dev \
      my-ros-timescaledb-env:latest \
      bash

    # OR Example: Mount just the src directory (requires running catkin_make inside)
    # docker run -it --rm \
    #  -v "/Users/tharunselvam/Documents/my_ros_project/ros1_ws/src:/root/ros1_ws/src" \
    #  --name ros1_ingestor_dev \
    #  my-ros-timescaledb-env:latest \
    #  bash
    ```
3.  **Build Inside Container:** Once inside the container, navigate to `/root/ros1_ws` and run `catkin_make`. It should succeed because all system dependencies (`libpqxx`, `nlohmann_json`, etc.) are already installed in the image.
    ```bash
    cd /root/ros1_ws
    catkin_make
    source devel/setup.bash
    # Now you can run roslaunch etc.
    ```

This `Dockerfile` creates a self-contained environment with everything needed to *build and run* your C++ ingestor node, making it much more reusable.
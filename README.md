# ROS 1 TimescaleDB Ingestor

## Overview

This repository contains a high-performance ROS 1 (Noetic) node designed for ingesting high-frequency sensor data streams into a TimescaleDB/PostgreSQL database. It aims to provide a robust and efficient bridge between ROS topics and time-series database storage, suitable for demanding robotics data logging applications.

**Key Features & Technologies:**

*   **High Throughput:** Engineered in C++ using `roscpp` to handle high message rates, leveraging a multi-threaded architecture to decouple database I/O from ROS message processing.
*   **Optimized Database Insertion:** Utilizes PostgreSQL's efficient `COPY FROM STDIN` command via the `libpqxx` C++ client library for fast bulk data insertion into TimescaleDB hypertables.
*   **Concurrency:** Employs a dedicated writer thread communicating with ROS callback threads via a thread-safe queue (`std::thread`, `std::mutex`, `std::condition_variable`, `std::queue`) to prevent I/O blocking.
*   **Configurability:** Database connection details, target ROS topic, message type (currently requires code changes for types other than `LaserScan`), target table, and batching parameters are configured via ROS parameters (YAML/launch files).
*   **Environment:** Developed and tested within a Docker container based on `ros:noetic-ros-base`, ensuring a reproducible environment with necessary dependencies (libpqxx, nlohmann/json).
*   **Current Focus:** The primary implementation handles `sensor_msgs/LaserScan` messages, converting array data to JSON strings for storage in `JSONB` columns (or native arrays if schema is modified).

---

## Database Setup

This node requires a running TimescaleDB (or PostgreSQL) instance and specific setup steps before launching the ingestor.

### Assumptions

*   A database (e.g., `robot_data`) exists and is accessible from the ROS node's environment.
*   The target hypertable exists within the database.
*   The hypertable schema includes columns corresponding to the fields specified in the node's configuration (`config/ingest_params.yaml`), including a `TIMESTAMPTZ NOT NULL` column for the primary time dimension.

### Example: `laser_scans` Table Setup

This example shows how to create a suitable hypertable for `sensor_msgs/LaserScan` data.

1.  **Connect to your database** (e.g., `robot_data`) using `psql` or a GUI tool:
    ```bash
    # Example psql connection
    psql -h <your_db_host_ip> -U robot_researcher -d robot_data
    # Default Password Used in Examples: secret
    ```

2.  **Create the table:**
    ```sql
    CREATE TABLE laser_scans (
        -- The primary timestamp column from header.stamp
        time TIMESTAMPTZ NOT NULL,

        -- Other scalar fields from the message
        frame_id TEXT,
        angle_min REAL,
        angle_max REAL,
        angle_increment REAL,
        time_increment REAL,
        scan_time REAL,
        range_min REAL,
        range_max REAL,

        -- Array fields stored as JSONB (requires nlohmann/json library)
        -- Alternatively, modify table schema to use REAL[] and update C++ node
        ranges JSONB,
        intensities JSONB
    );

    -- Optional comments
    COMMENT ON COLUMN laser_scans.time IS 'Timestamp from ROS message header (header.stamp)';
    COMMENT ON COLUMN laser_scans.ranges IS 'Laser scan range data [m], stored as a JSON array';
    ```

3.  **Convert to Hypertable:**
    ```sql
    SELECT create_hypertable('laser_scans', 'time');
    ```

4.  **(Recommended) Add Indexes:**
    ```sql
    -- Index on frequently queried columns (time index is created automatically)
    CREATE INDEX ON laser_scans (frame_id, time DESC);
    ```

---

## Docker Environment Setup

A `Dockerfile` is provided to create a reusable Docker image containing all necessary dependencies (ROS Noetic, libpqxx, nlohmann-json, etc.) to build and run the C++ ingestor node.

1.  **Build the Docker Image:** Navigate to the repository root directory (containing the `Dockerfile`) in your terminal and run:
    ```bash
    docker build -t ros-timescaledb-ingestor-env:latest .
    # You can change the image name/tag 'ros-timescaledb-ingestor-env:latest'
    ```

2.  **Run a Container:** Start a container from the built image, mounting your local workspace directory into the container.
    ```bash
    # Example: Mount the whole workspace from host into /root/ros1_ws in container
    docker run -it --rm \
      -v "/path/on/your/host/to/this/repo/ros1_ws:/root/ros1_ws" \
      --name ros1_ingestor_dev \
      ros-timescaledb-ingestor-env:latest \
      bash
    # Replace '/path/on/your/host/to/this/repo/ros1_ws' with the correct path
    ```
    *Note: Ensure the volume mount points to the `ros1_ws` directory within this repository on your host machine.*

---

## Usage

1.  **Build the Node:** Inside the running container (from the previous step), navigate to the workspace and build using `catkin_make`:
    ```bash
    cd /root/ros1_ws
    catkin_make
    ```

2.  **Source the Workspace:** In *every terminal* used inside the container, source the setup file:
    ```bash
    source /root/ros1_ws/devel/setup.bash
    ```

3.  **Configure:** Edit the `config/ingest_params.yaml` file within the workspace (`/root/ros1_ws/src/timescaledb_ingestor/config/ingest_params.yaml`) to match your specific database credentials, target topic name, message type details, and table/column names.

4.  **Run:**
    *   **Terminal 1:** `roscore`
    *   **Terminal 2:** Launch the ingestor node:
        ```bash
        # Use full path if needed to avoid ambiguity
        roslaunch /root/ros1_ws/src/timescaledb_ingestor/launch/ingestor.launch
        ```
    *   **Terminal 3:** Publish data to the configured ROS topic (e.g., using `rostopic pub` or another ROS node).

---

## TO DO

*   Implement a Python script within the package to automatically create the required hypertable and indexes based on configuration, reducing manual setup.
*   Generalize the C++ node to handle different message types more dynamically (potentially using message introspection or code generation, significantly more complex).
*   Add more robust error handling and potential data reprocessing for failed database batches.
*   Explore native PostgreSQL array types (`REAL[]`) instead of `JSONB` for potential performance gains in the C++ node, requiring schema and C++ formatting changes.
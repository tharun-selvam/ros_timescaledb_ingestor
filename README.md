## Log in to DB

```bash
psql -h 10.102.75.119 -U robot_researcher -d robot_data
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

-- Optional: Add comments
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
- add threading to handle data flushing and message handling
- make flushing non blocking
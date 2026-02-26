-- RustLake Demo: Cassandra seed data
-- Wide-column patterns for time-series and IoT workloads

CREATE KEYSPACE IF NOT EXISTS rustlake_demo
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE rustlake_demo;

-- Sensor readings (time-series, partitioned by device + day)
CREATE TABLE IF NOT EXISTS sensor_readings (
    device_id text,
    day date,
    timestamp timestamp,
    temperature double,
    humidity double,
    pressure double,
    battery_pct int,
    PRIMARY KEY ((device_id, day), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

INSERT INTO sensor_readings (device_id, day, timestamp, temperature, humidity, pressure, battery_pct)
VALUES ('sensor-001', '2024-06-01', '2024-06-01 10:00:00+0000', 22.5, 45.2, 1013.25, 95);
INSERT INTO sensor_readings (device_id, day, timestamp, temperature, humidity, pressure, battery_pct)
VALUES ('sensor-001', '2024-06-01', '2024-06-01 10:05:00+0000', 22.7, 44.8, 1013.20, 95);
INSERT INTO sensor_readings (device_id, day, timestamp, temperature, humidity, pressure, battery_pct)
VALUES ('sensor-001', '2024-06-01', '2024-06-01 10:10:00+0000', 23.1, 43.5, 1013.15, 94);
INSERT INTO sensor_readings (device_id, day, timestamp, temperature, humidity, pressure, battery_pct)
VALUES ('sensor-002', '2024-06-01', '2024-06-01 10:00:00+0000', 19.8, 52.1, 1012.80, 87);
INSERT INTO sensor_readings (device_id, day, timestamp, temperature, humidity, pressure, battery_pct)
VALUES ('sensor-002', '2024-06-01', '2024-06-01 10:05:00+0000', 19.9, 51.8, 1012.75, 87);
INSERT INTO sensor_readings (device_id, day, timestamp, temperature, humidity, pressure, battery_pct)
VALUES ('sensor-003', '2024-06-01', '2024-06-01 10:00:00+0000', 25.3, 38.9, 1014.10, 72);

-- User activity log (write-heavy append pattern)
CREATE TABLE IF NOT EXISTS user_activity (
    user_id text,
    activity_date date,
    timestamp timestamp,
    action text,
    details text,
    PRIMARY KEY ((user_id), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

INSERT INTO user_activity (user_id, activity_date, timestamp, action, details)
VALUES ('user-001', '2024-06-01', '2024-06-01 10:00:00+0000', 'login', 'web browser');
INSERT INTO user_activity (user_id, activity_date, timestamp, action, details)
VALUES ('user-001', '2024-06-01', '2024-06-01 10:15:00+0000', 'query', 'SELECT * FROM orders');
INSERT INTO user_activity (user_id, activity_date, timestamp, action, details)
VALUES ('user-002', '2024-06-01', '2024-06-01 09:30:00+0000', 'login', 'mobile app');
INSERT INTO user_activity (user_id, activity_date, timestamp, action, details)
VALUES ('user-002', '2024-06-01', '2024-06-01 09:45:00+0000', 'upload', 'sales_q2.csv');

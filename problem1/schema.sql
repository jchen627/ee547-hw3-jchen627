-- problem1/schema.sql
-- Schema for Metro Transit Database

DROP TABLE IF EXISTS stop_events CASCADE;
DROP TABLE IF EXISTS trips CASCADE;
DROP TABLE IF EXISTS line_stops CASCADE;
DROP TABLE IF EXISTS stops CASCADE;
DROP TABLE IF EXISTS lines CASCADE;

CREATE TABLE lines (
    line_id SERIAL PRIMARY KEY,
    line_name VARCHAR(50) NOT NULL UNIQUE,
    vehicle_type VARCHAR(10) NOT NULL CHECK (vehicle_type IN ('rail', 'bus'))
);

CREATE TABLE stops (
    stop_id SERIAL PRIMARY KEY,
    stop_name VARCHAR(120) NOT NULL UNIQUE,
    latitude NUMERIC(9,6) NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude NUMERIC(9,6) NOT NULL CHECK (longitude BETWEEN -180 AND 180)
);

-- A line has many stops in order; a stop can appear on multiple lines
CREATE TABLE line_stops (
    line_id INTEGER NOT NULL REFERENCES lines(line_id) ON DELETE CASCADE,
    stop_id INTEGER NOT NULL REFERENCES stops(stop_id) ON DELETE CASCADE,
    sequence_number INTEGER NOT NULL CHECK (sequence_number >= 1),
    time_offset_minutes INTEGER NOT NULL DEFAULT 0 CHECK (time_offset_minutes >= 0),
    PRIMARY KEY (line_id, sequence_number)
   
);

-- Trips are scheduled runs of a single line
CREATE TABLE trips (
    trip_id VARCHAR(20) PRIMARY KEY,
    line_id INTEGER NOT NULL REFERENCES lines(line_id) ON DELETE RESTRICT,
    scheduled_departure TIMESTAMP NOT NULL,
    vehicle_id VARCHAR(20) NOT NULL,
    -- A vehicle shouldn't have two trips on the same line at the exact same time
    UNIQUE (line_id, scheduled_departure, vehicle_id)
);

-- Actual stop events observed during trips
CREATE TABLE stop_events (
    trip_id VARCHAR(20) NOT NULL REFERENCES trips(trip_id) ON DELETE CASCADE,
    stop_id INTEGER NOT NULL REFERENCES stops(stop_id) ON DELETE RESTRICT,
    scheduled TIMESTAMP NOT NULL,
    actual TIMESTAMP NOT NULL,
    passengers_on INTEGER NOT NULL DEFAULT 0 CHECK (passengers_on >= 0),
    passengers_off INTEGER NOT NULL DEFAULT 0 CHECK (passengers_off >= 0),
    PRIMARY KEY (trip_id, stop_id)
);

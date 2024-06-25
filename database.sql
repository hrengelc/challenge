-- Create the database
CREATE DATABASE etl;

-- Connect to the database
\c etl;

-- Create the events table
CREATE TABLE events (
    GLOBALEVENTID BIGINT PRIMARY KEY,
    SQLDATE DATE,
    EventCode VARCHAR(10),
    EventBaseCode VARCHAR(10),
    EventRootCode VARCHAR(10),
    ActionGeo_FullName VARCHAR(255),
    ActionGeo_CountryCode VARCHAR(10),
    ActionGeo_Lat DECIMAL(10, 7),
    ActionGeo_Long DECIMAL(10, 7),
    DATEADDED TIMESTAMP,
    SOURCEURL TEXT
);
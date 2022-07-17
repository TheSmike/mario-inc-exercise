CREATE TABLE IF NOT EXISTS mario.raw_device_data (
  received DATE,
  device STRING,
  timestamp STRING,
  CO2_level INTEGER,
  humidity INTEGER,
  temperature INTEGER
)
USING DELTA
  PARTITIONED BY (received_date)
LOCATION 'target/tmp/mario/raw_device_data/'

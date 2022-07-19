CREATE TABLE IF NOT EXISTS mario.raw_device_data (
  received DATE,
  device STRING,
  timestamp STRING,
  CO2_level BIGINT,
  humidity BIGINT,
  temperature BIGINT
)
USING DELTA
  PARTITIONED BY (received)
LOCATION '/Users/mscarpenti/data/mario/raw_device_data/'

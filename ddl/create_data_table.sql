CREATE TABLE IF NOT EXISTS mario.raw_device_data (
  received DATE,
  device STRING,
  timestamp STRING,
  CO2_level INTEGER,
  humidity INTEGER,
  temperature INTEGER
)
USING DELTA
  PARTITIONED BY (received)
LOCATION '/Users/mscarpenti/data/mario/device_data/'

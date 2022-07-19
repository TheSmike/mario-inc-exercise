CREATE TABLE IF NOT EXISTS mario.device_info (
  code STRING,
  type STRING,
  area STRING,
  customer STRING
)
USING DELTA
LOCATION '/Users/mscarpenti/data/mario/device_info/'
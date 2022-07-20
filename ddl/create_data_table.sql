CREATE TABLE IF NOT EXISTS mario.device_data (
  received_date DATE,
  event_timestamp TIMESTAMP,
  device STRING,
  CO2_level BIGINT,
  humidity BIGINT,
  temperature BIGINT,
  event_date DATE GENERATED ALWAYS AS (CAST(event_timestamp AS DATE)),
)
USING DELTA
  PARTITIONED BY (event_date, device)
LOCATION '/Users/mscarpenti/data/mario/device_data/'

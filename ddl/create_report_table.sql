CREATE OR REPLACE TABLE mario.device_report (
  year_month STRING,
  area STRING,
  CO2_level_avg DOUBLE,
  humidity_avg DOUBLE,
  temperature_avg DOUBLE
)
USING DELTA
LOCATION '/Users/mscarpenti/data/mario/device_report/'
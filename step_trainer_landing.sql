CREATE EXTERNAL TABLE IF NOT EXISTS `toantd19`.`step_trainer_landing` (
  `serialnumber` string,            -- Serial number of the device (string)
  `timestamp` bigint,              -- Unix timestamp for the sensor reading (bigint)
  `steps` bigint,                  -- Number of steps (bigint)
  `distance` double,               -- Distance covered (double)
  `calories` double,               -- Calories burned (double)
  `heartRate` float,               -- Heart rate in bpm (float)
  `batteryLevel` float,            -- Battery level of the device (float)
  `sensorReadingTime` bigint,      -- Timestamp of the sensor reading (bigint)
  `user` string                    -- User identifier (string, could be email or user ID)
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://toantd19-bucket/step_trainer/landing/'
TBLPROPERTIES ('classification' = 'json');


CREATE EXTERNAL TABLE IF NOT EXISTS `toantd19`.`accelerometer_landin g` (
  `user_id` string,
  `event_timestamp` bigint,
  `acceleration_x` float,
  `acceleration_y` float,
  `acceleration_z` float
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
LOCATION 's3://toantd19-bucket/accelerometer/landing/‘
TBLPROPERTIES ('classification' = 'json');


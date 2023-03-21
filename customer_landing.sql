CREATE EXTERNAL TABLE `stedi_customer_landing`(
  `serialnumber` string COMMENT 'from deserializer', 
  `customername` string COMMENT 'from deserializer', 
  `email` string COMMENT 'from deserializer', 
  `phone` bigint COMMENT 'from deserializer', 
  `birthday` date COMMENT 'from deserializer', 
  `registrationdate` bigint COMMENT 'from deserializer', 
  `lastupdatedate` bigint COMMENT 'from deserializer', 
  `sharewithresearchasofdate` bigint COMMENT 'from deserializer', 
  `sharewithpublicasofdate` bigint COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'paths'='birthDay,customerName,email,lastUpdateDate,phone,registrationDate,serialNumber,shareWithPublicAsOfDate,shareWithResearchAsOfDate') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://udacity-stedi/customer/landing/'
TBLPROPERTIES (
  'classification'='json')
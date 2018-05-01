hadoop fs -put target/hive_udf-2.0.jar hdfs:///ExBigData
hive -f "ADD JAR hdfs:///ExBigData/hive_udf-2.0.jar"
hive -e "CREATE TEMPORARY FUNCITON zodiac AS 'brucedu.bigdata.hive_udf.udf.UDFZodiacSign'"
hive -e "CREATE TEMPORARY FUNCTION nvl AS 'brucedu.bigdata.hive_udf.udf.GenericUDFNvl'"
hive -e "CREATE TEMPORARY FUNCTION "
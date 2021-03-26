package ca.mcit.bigdata.hive

object HiveTableManager extends HiveClient {

  private val hdfsRootDir = "/user/bdsf2001/manik/project4"

  def createNewTables(): Unit = {

    dropExistingTable("ext_routes")
    stmt.executeUpdate(
      s"""CREATE EXTERNAL TABLE IF NOT EXISTS ext_routes (
         |    route_id INT,
         |    agency_id STRING,
         |    route_short_name INT,
         |    route_long_name STRING,
         |    route_type INT,
         |    route_url STRING,
         |    route_color STRING,
         |    route_text_color STRING
         | )
         | ROW FORMAT DELIMITED
         | FIELDS TERMINATED BY ','
         | LOCATION '$hdfsRootDir/routes'
         | TBLPROPERTIES (
         | 'skip.header.line.count' = '1' ,
         | 'serialization.null.format' = ''
         | )""".stripMargin)

    dropExistingTable("ext_trips")
    stmt.executeUpdate(
      s"""CREATE EXTERNAL TABLE IF NOT EXISTS ext_trips (
         |    route_id INT,
         |    service_id STRING,
         |    trip_id STRING,
         |    trip_headsign STRING,
         |    direction_id INT,
         |    shape_id INT,
         |    wheelchair_accessible INT
         | )
         | ROW FORMAT DELIMITED
         | FIELDS TERMINATED BY ','
         | LOCATION '$hdfsRootDir/trips'
         | TBLPROPERTIES (
         | 'skip.header.line.count' = '1' ,
         | 'serialization.null.format' = ''
         | )""".stripMargin)

    dropExistingTable("ext_calendar_dates")
    stmt.executeUpdate(
      s"""CREATE EXTERNAL TABLE IF NOT EXISTS ext_calendar_dates (
         |    service_id STRING,
         |    date STRING,
         |    exception_type INT
         | )
         | ROW FORMAT DELIMITED
         | FIELDS TERMINATED BY ','
         | LOCATION '$hdfsRootDir/calendar_dates'
         | TBLPROPERTIES (
         | 'skip.header.line.count' = '1' ,
         | 'serialization.null.format' = ''
         | )""".stripMargin)

    stmt.executeUpdate(
      s"""CREATE TABLE IF NOT EXISTS enriched_trips (
         |    trip_id STRING,
         |    service_id STRING,
         |    route_id INT,
         |    trip_headsign STRING,
         |    date STRING,
         |    exception_type INT,
         |    route_long_name STRING,
         |    route_color STRING
         | )
         | PARTITIONED BY (wheelchair_accessible BOOLEAN)
         | STORED as PARQUET
         | TBLPROPERTIES (
         | 'parquet.compression'='GZIP'
         | )""".stripMargin)

    stmt.close()
  }

  def dropExistingTable(tableName: String): Unit = {
    val dropTableSQL = s"DROP TABLE IF EXISTS $tableName"
    stmt.execute(dropTableSQL)
  }
}

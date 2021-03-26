package ca.mcit.bigdata.hive

object AppMain extends App with HiveClient {

  AutoStagingAndSchemaDeploy.extractAndUnzipSTMFiles()
  AutoStagingAndSchemaDeploy.loadFilesToHDFS()
  HiveTableManager.createNewTables()

  val enableNonStrictGzipMode =
    s"""SET hive.exec.dynamic.partition.mode=nonstrict
       | SET hive.exec.dynamic.partition=nonstrict
       | SET hive.mapred.mode=nonstrict
       | SET hive.exec.compress.output=true
       | SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec""".stripMargin
  stmt.execute(enableNonStrictGzipMode)

  val joinOptimizationMode =
    s"""SET hive.auto.convert.join=true
       | SET hive.mapjoin.smalltable.filesize=10000000""".stripMargin
  stmt.execute(joinOptimizationMode)

  val hiveInLocalMode = s"""SET mapreduce.framework.name=local""".stripMargin
  stmt.executeUpdate(hiveInLocalMode)

  val enrichedTripsSQL =
    s"""INSERT INTO enriched_trips PARTITION (wheelchair_accessible)
       | SELECT /*+ STREAMTABLE(trips) */
       |    trips.trip_id,
       |    calender.service_id,
       |    routes.route_id,
       |    trips.trip_headsign,
       |    calender.`date`,
       |    calender.exception_type,
       |    routes.route_long_name,
       |    routes.route_color,
       |    CASE WHEN trips.wheelchair_accessible = 1 THEN TRUE ELSE FALSE END wheelchair_accessible
       | FROM ext_trips trips
       | LEFT JOIN ext_routes routes
       | ON trips.route_id = routes.route_id
       | LEFT JOIN ext_calendar_dates calender
       | ON trips.service_id = calender.service_id""".stripMargin

  stmt.executeUpdate(enrichedTripsSQL)
  stmt.close()
  connection.close()
}

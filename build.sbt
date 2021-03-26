name := "course4project"

version := "0.1"

scalaVersion := "2.12.12"

val hadoopVersion = "2.7.7"
val jdbcVersion = "1.1.0-cdh5.16.2"

val hadoopDeps = Seq(
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion
)
val hiveDeps = Seq(
  "org.apache.hive" % "hive-jdbc" % jdbcVersion
)

libraryDependencies ++=hadoopDeps++ hiveDeps
resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

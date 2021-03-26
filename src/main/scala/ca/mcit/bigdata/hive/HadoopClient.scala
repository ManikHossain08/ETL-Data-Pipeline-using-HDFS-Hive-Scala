package ca.mcit.bigdata.hive

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait HadoopClient {

  val stagingDir = "/user/bdsf2001/manik/project4"
  val conf = new Configuration()
  val hadoopConfDir: String = System.getenv("HADOOP_CONF_DIR")
  conf.addResource(new Path(s"$hadoopConfDir/core-site.xml"))
  conf.addResource(new Path(s"$hadoopConfDir/hdfs-site.xml"))

  val fileSystem: FileSystem = FileSystem.get(conf)
}

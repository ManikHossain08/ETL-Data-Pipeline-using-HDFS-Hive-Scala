package ca.mcit.bigdata.hive

import org.apache.hadoop.fs.Path
import scala.collection.immutable.HashMap
import sys.process._

object AutoStagingAndSchemaDeploy extends HadoopClient {

  private val localSysDir = "./data"
  private val stmFilesWebURL = "http://www.stm.info/sites/default/files/gtfs/gtfs_stm.zip"
  private val listOfStagingDirFiles = HashMap(
    "trips" -> "trips.txt", "routes" -> "routes.txt", "calendar_dates" -> "calendar_dates.txt"
  )

  def extractAndUnzipSTMFiles(): Unit = {
    s"rm -r $localSysDir".!
    s"wget $stmFilesWebURL -P $localSysDir/".!
    s"unzip -q $localSysDir/gtfs_stm.zip -d $localSysDir/".!
    s"rm $localSysDir/gtfs_stm.zip".!
  }

  def loadFilesToHDFS(): Unit = {
    deleteHdfsExistingDirectory(stagingDir)
    listOfStagingDirFiles.foreach {
      case (folderName, fileName) =>
        fileSystem.mkdirs(new Path(s"$stagingDir/$folderName"))
        fileSystem.copyFromLocalFile(
          new Path(s"$localSysDir/$fileName"), new Path(s"$stagingDir/$folderName")
        )
    }
  }

  def deleteHdfsExistingDirectory(folderPath: String): Unit = {
    val pathHDFS = new Path(folderPath)
    if (fileSystem.exists(pathHDFS)) {
      fileSystem.delete(pathHDFS, true)
    }
  }
}

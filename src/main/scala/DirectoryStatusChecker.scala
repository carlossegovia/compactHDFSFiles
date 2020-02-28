import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  *
  * @param spark           : Spark session
  * @param hdfsBlockSizeMB : HDFS block size in MegaBytes
  */
class DirectoryStatusChecker(spark: SparkSession, hdfsBlockSizeMB: Long = 128) {

  val listDirModTime: mutable.HashMap[String, Long] = collection.mutable.HashMap[String,Long]()
  def checkModificationTime(uriPath: String): Unit = {
    val path: Path = new Path(uriPath)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val listStatus = fs.listStatus(path).filter(_.isDirectory)
    if (listStatus.length > 0) {
      listStatus.map(_.getPath.toUri.toString).foreach(checkModificationTime) // recursive call
    } else {
      val status = fs.getFileStatus(path)
      listDirModTime += uriPath -> status.getModificationTime
    }
  }

  def checkFilesSize(uriPath: String): Unit = {
    val path: Path = new Path(uriPath)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val status = fs.listStatus(path).filter(_.isDirectory)
    if (status.length > 0) {
      status.map(_.getPath.toUri.toString).foreach(checkFilesSize) // recursive call
    } else {
      println(uriPath)
    }
  }

  //  TODO
  // 1 - Recorrer hojas y determinar si deben ser comprimidos o no.
  // 2 - Recorrer hojas y determinar su tipo de dato y compresion/o no compresion.
  // 3 - Recorrer directorios y ordenar segun su ultima modificacion.
}

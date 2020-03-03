import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable

/**
  *
  * @param spark : Spark session
  */
class DirectoryStatusChecker(spark: SparkSession) {


  /**
    * Return a Spark Dataframe with data about the leaf directories in a root directory
    *
    * @param uriPath : The HDFS URI path of the root directory
    * @return
    */
  def getDataframeStats(uriPath: String): DataFrame = {
    val MBytes: Int = 1024 * 1024
    val listDirStats: mutable.ListBuffer[DirectoryStats] = collection.mutable.ListBuffer[DirectoryStats]()

    /**
      * Recursive method for collect stats of the leaf directories
      *
      * @param uriPath : An HDFS URI path
      */
    def getDirStats(uriPath: String): Unit = {
      val path: Path = new Path(uriPath)
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      // Return a list of directories if they're valid. For example, directories that start with  '.metadata' or
      // '.signals' aren't valid.
      val invalidPrefixList = List("_", "-", ".")
      val listStatus = fs.listStatus(path).filter(_.isDirectory).filter(dir => {
        !invalidPrefixList.exists(prefix => dir.getPath.toUri.toString.split("/").last.startsWith(prefix))
      })
      if (listStatus.length > 0) {
        listStatus.map(_.getPath.toUri.toString).foreach(getDirStats) // recursive call
      } else {
        val status = fs.getFileStatus(path)
        // Return the list of parquet files in the directory
        val listParquetFiles = fs.listStatus(path).filter(_.isFile).filter(_.getPath.toUri.toString.contains("" +
          ".parquet"))
        // True if the directory contains files without gzip compression.
        val hasUncompressedFiles = listParquetFiles.exists(!_.getPath.toUri.toString.contains(".gz."))
        // The average size of parquet files in the directory
        val avgFilesSize = if (listParquetFiles.length != 0) fs.getContentSummary(path).getLength / listParquetFiles
          .length else fs.getContentSummary(path).getLength
        // Add the stats to the list
        listDirStats += DirectoryStats(uriPath, status.getModificationTime, status.getAccessTime, status.getOwner, fs
          .getContentSummary(path).getLength, listParquetFiles.length, avgFilesSize, containsUncompressedFiles =
          hasUncompressedFiles)
      }
    }

    getDirStats(uriPath)
    // Transform the List of DirectoryStats in a Dataset
    import spark.implicits._
    val ds: Dataset[DirectoryStats] = listDirStats.toDS()
    // Add other columns with more human-readable data
    val df = ds.withColumn("avgParquetFilesMBytes", ds("avgParquetFilesBytes") / MBytes)
      .withColumn("dirSizeMBytes", ds("dirSizeBytes") / MBytes)
      .withColumn("lastModTimestamp", (ds("lastModTime") / 1000).cast(TimestampType))
      .withColumn("lastAccessTimestamp", (ds("lastAccessTime") / 1000).cast(TimestampType))
    // Rename the columns name. Replace CamelCase por SnakeCase
    val colsNamesWithUnderscores = df.columns.map(Utils.camelToUnderscores)
    df.toDF(colsNamesWithUnderscores: _*)
  }
}

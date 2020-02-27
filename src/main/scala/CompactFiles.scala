import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

class CompactFiles(spark: SparkSession, hdfsBlockSizeMB: Long = 128, tempSuffixe: String = "_compact_temp") {

  /**
    * Compact existing parquet files in an HDFS directory into parquet files with an approximate size of an HDFS block.
    *
    * @param uriPath : HDFS directory path
    */
  def compactParquetFiles(uriPath: String): Unit = {
    val hdfsBlockSizeBytes = hdfsBlockSizeMB * 1024 * 1024
    val path: Path = new Path(uriPath)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val dirSize = fs.getContentSummary(path).getLength
    val fileNum = (if (dirSize / hdfsBlockSizeBytes > 1) dirSize / hdfsBlockSizeBytes else 1).toInt
    // Read the data and then store it in a temporary directory
    val df = spark.sqlContext.read.parquet(uriPath)
    df.repartition(fileNum).write.mode("overwrite").option("compression", "gzip")
      .parquet(uriPath + tempSuffixe)
    // Delete the original directory and then rename the temp directory like the original
    fs.delete(new Path(uriPath), true)
    fs.rename(new Path(uriPath + tempSuffixe), new Path(uriPath))
  }

  /**
    * Receive an HDFS path and compact the parquet files in the leaf directories.
    *
    * @param uriPath : HDFS directory path
    */
  def compact(uriPath: String): Unit = {
    val path: Path = new Path(uriPath)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val status = fs.listStatus(path).filter(_.isDirectory)
    if (status.length > 0) {
      status.map(_.getPath.toUri.toString).foreach(compact) // recursive call
    } else {
      compactParquetFiles(uriPath)
    }
  }

}
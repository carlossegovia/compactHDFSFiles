import org.apache.spark.sql.SparkSession


object Compactor {
  def main(args: Array[String]): Unit = {
    if (args.length > 0) {
      val spark = SparkSession.builder.appName("CompactFiles").getOrCreate()
      val cf = new CompactFiles
      cf.compactParquetFiles(spark, args(0))
      spark.stop()
    } else {
      println("ERROR! It's needed at least one argument to compact the HDFS files." +
        "\n-1st argument (Required): HDFS directory path" +
        "\n-2nd argument (Optional): HDFS block size in MB")
    }
  }
}

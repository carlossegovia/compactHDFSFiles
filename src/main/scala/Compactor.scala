import org.apache.spark.sql.SparkSession

object Compactor {
  val usage =
    """
    Usage: CompactHDFSFiles.Jar [-f number] [-t string] [-s number] [-r string]
    Required: [-f number] [-t string]
    Optional: [-s number] [-r string]

    Description:
    -f: Function number.  (1) for compact parquet files or (2) for collect directories stats.
    -t: Target path.      The HDFS path for compact the parquet files or collect stats.
    -r: Result path.      The HDFS path for store the collected stats. Default, '/user/collected_stats/'
    -s: HDFS block size.  The HDFS block size in Megabytes for compact the data. Default, 128 MB.

    """

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println(usage)
      throw new Exception("ERROR! It's needed at least two arguments for executing the app.\n")
    }
    val argList = args.toList
    type OptionMap = Map[String, String]

    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      list match {
        case Nil => map
        case "-f" :: value :: tail =>
          nextOption(map ++ Map("function" -> value), tail)
        case "-t" :: value :: tail =>
          nextOption(map ++ Map("target_path" -> value), tail)
        case "-r" :: value :: tail =>
          nextOption(map ++ Map("result_path" -> value), tail)
        case "-s" :: value :: tail =>
          nextOption(map ++ Map("hdfs_size" -> value), tail)
        case option :: tail => println("Unknown option " + option)
          throw new Exception("Unknown option\n")
      }
    }

    val options = nextOption(Map(), argList)

    if (options.contains("function") && options.contains("target_path")) {
      val spark = SparkSession.builder.appName("CompactFiles").getOrCreate()
      options("function").toInt match {
        case 1 =>
          val blockSize = if (options.contains("hdfs_size")) options("hdfs_size").toLong else 128.toLong
          println(blockSize)
          val cf = new CompactFiles(spark, blockSize)
          cf.compact(options("target_path"))
        case 2 =>
          val dsc = new DirectoryStatusChecker(spark)
          val df = dsc.getDataframeStats(options("target_path"))
          val pathResult = if (options.contains("result_path")) options("result_path") else "/user/collected_stats/"
          println(pathResult)
          //Store the collected results
          df.repartition(1).write.mode("overwrite").option("compression", "gzip")
            .parquet(pathResult)
        case other =>
          println(usage)
          throw new Exception(s"Option $other doesn't exist. Select option 1 or 2.\n")
      }
      spark.stop()
    } else {
      println(usage)
      throw new Exception("ERROR! The arguments -f and -t are required.")
    }


  }
}

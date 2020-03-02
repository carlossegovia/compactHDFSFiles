import java.text.SimpleDateFormat

/**
  * A Class that represents the stats of a Directory
  * @param uriPath: The HDFS URI path of an leaf directory
  * @param lastModTime: The last modification time of the directory
  * @param lastAccessTime: The last accesses time of the directory
  * @param owner: The owner of the directory
  * @param dirSizeBytes: Total size in bytes of the directory
  * @param countParquetFiles: Number of parquet files in the directory
  * @param avgParquetFilesBytes: Average size of parquet files in the directory
  * @param containsUncompressedFiles: True if the directory has uncompressed parquet files
  */
case class DirectoryStats(uriPath: String, lastModTime: Long, lastAccessTime: Long, owner: String, dirSizeBytes: Long,
                          countParquetFiles: Int, avgParquetFilesBytes: Long, containsUncompressedFiles: Boolean) {

  val sdf = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss")

  def getAvgParquetFilesMBytes: Long = this.avgParquetFilesBytes * 1024 * 1024

  def getDirSizeMBytes: Long = this.dirSizeBytes * 1024 * 1024

  def getLastModTimeString: String = sdf.format(this.lastModTime)

  def getLastAccessTimeString: String = sdf.format(this.lastAccessTime)

  def getUriPath: String = this.uriPath

  def getLastModTime: Long = this.lastModTime

  def getLastAccessTime: Long = this.lastAccessTime

  def getOwner: String = this.owner

  def getDirSizeBytes: Long = this.dirSizeBytes

  def getCountParquetFiles: Int = this.countParquetFiles

  def getParquetFilesBytes: Long = this.avgParquetFilesBytes

}

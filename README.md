# Compact HDFS Files

CompactHDFSFiles is a project in Scala to compact small HDFS files into larger ones.
Right now, the project receive as an argument an HDFS directory path with small parquet files and compact them into larger ones.

## Small files problem in HDFS

 A file which is less than HDFS block size (64MB/128MB) is termed as small file. NameNode stores all files metadata in memory, so if you are storing lots of small files, NameNode has to maintain its metadata, for a file metadata, it occupies 150 bytes so in the case of million files it would cost around 3GB of memory. So, a large number of small files will end up using a lot of memory of the master and scaling up in this fashion is not feasible. When there are large number of files, there will be a lot of seeks on the disk as frequent hopping from data node to data node will be done and hence increasing the file read/write time.

## Tech
* Scala 2.11
* SBT 1.3
* Apache Spark 2.2.1

## How to use? 

Assuming you have a Cluster with a functionally environment of Apache Spark, you just need to download the project and generate the JAR. 

1. Clone the project:
```sh
$ git clone https://github.com/carlossegovia/compactHDFSFiles.git
```
2. Compile the project with SBT:
```sh
$ sbt clean package
```
*The JAR file will be generate in __./compactHDFSFiles/target/scala-2.11/CompactHDFSFiles.jar__*

3. Execute the JAR with Apache Spark:
```sh
$ spark-submit --master yarn --deploy-mode cluster --name CompactHDFSFiles --class Compactor hdfs://HDFSpathToJAR/CompactHDFSFiles.jar hdfs://pathOfDirectoryToCompact
```
*The object Compactor can receive two arguments:*
* **(Required)**: HDFS directory path
* **(Optional)**: HDFS block size in MB. The default value is 128 MB.
        

### Todos

 - Write Tests
 - Write methods to support more types of files.


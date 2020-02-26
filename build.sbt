name := "compactHDFSFiles"

version := "0.1"

scalaVersion := "2.11.12"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

// name of the package
artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  "CompactHDFSFiles" + "." + artifact.extension
}
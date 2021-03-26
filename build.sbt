name := "SPARK_HDFS_MANIP"

version := "0.1"

scalaVersion := "2.11.12"

resolvers ++= Seq("Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/")

//resolvers ++= Seq("spark hadoopoffice" at "https://mvnrepository.com/artifact/com.github.zuinnote/spark-hadoopoffice-ds")


libraryDependencies ++= Seq(
  "org.apache.spark"     %% "spark-core" % "2.4.0-cdh6.3.4" ,
  "org.apache.spark"     %% "spark-sql"  % "2.4.0-cdh6.3.4" ,
)

//libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.4.0"

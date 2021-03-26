package com.example

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd

object SparkHdfsFileManip extends HADOOP_FILE_INTERFACE {
  def main(args: Array[String]): Unit = {


  }

  override val spark: SparkSession = SparkSession.builder().getOrCreate()

  import spark.implicits._
  override def listFilesNames(hdfsPath: String): Array[String] = {
    val fs= FileSystem.get(new Configuration())
    val path = new Path("/user/bnahali/LAST_USAGE_OFFNET")
    val repoContent=fs.listStatus(path)
    repoContent.filter(!_.isDirectory).map(_.getPath.getName)
  }

  override def listFilesWithExtension(hdfsPath: String, fileExtension: String): Array[String] = {
    val fs= FileSystem.get(new Configuration())
    val path = new Path(hdfsPath)
    val pathFilter = new PathFilter {
      override def accept(path: Path): Boolean = path.getName.endsWith(fileExtension)
    }
    fs.listStatus(path,pathFilter).filter(!_.isDirectory).map(_.getPath.getName)
  }

  override def getLatestFiles(hdfsPath: String, numberFiles: Int): DataFrame = {
    val fs= FileSystem.get(new Configuration())
    val path = new Path(hdfsPath)
    val fileArray=fs.listStatus(path).filter(!_.isDirectory)
    val latestFilesList=fileArray.sortBy(p=> p.getModificationTime)(Ordering[Long].reverse).map(_.getPath.getName).take(numberFiles).toList
    val sc=spark.sparkContext
    sc.textFile(latestFilesList.mkString(",")).toDF()
    //val fileDf = spark.sparkContext.parallelize(latestFilesList).toDF("file name")
  }

  override def renameFiles(hdfsPath: String, renameSuffix: String): Any = {
    val fs = FileSystem.get(new Configuration())
    val files = fs.listStatus(new Path(hdfsPath))
    files.map(_.getPath).foreach(p => fs.rename(p,p.suffix(renameSuffix)))
  }

  //Apply try and catch for file format
  override def readFileWithFormat(fileFormat: String, optionsMap:Map[String,String],schema:StructType,hdfsPath: String): DataFrame = {
    val formatList=List("json","csv","text",  "parquet","orc") //Avro format needs a special package

    if( formatList.contains(fileFormat)){
      return spark.read.format(fileFormat).options(optionsMap).schema(schema).load(hdfsPath)
    }
    else return spark.createDataFrame(spark.sparkContext
      .emptyRDD[Row], schema)


    //format, options,  schema
  }

}





//constructer in scala
//string manip in scala
//how to use a variable in spark scala code




//reading json into dataframe
//fuse the two list file functions into one
//why should we implement accept in PathFilter
//scala polymorphisme and overloaded methods
// scala difference between sort by and sort with
// try using spark.read.textFile for multiple files using a separator
// I have doubts with regard to textFile for multiple files

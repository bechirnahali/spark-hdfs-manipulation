package com.example

import org.apache.hadoop.fs.Path
import org.apache.spark.sql._


trait HADOOP_FILE_INTERFACE {
  def listFilesNames(hdfsPath:String):Array[String]
  def listFilesWithExtension(hdfsPath:String,fileExtension:String):Array[String]
  def getLatestFiles(hdfsPath:String,numberFiles:Int):DataFrame//returns a dataframe which contains data related to the n latest files.
  def getFilesWithExtension()
  def renameFiles(hdfsPath:String,renameTag:String):Any
  def readFileWithFormat(fileFormat:String,options:Map[String,String],schema:StructType,hdfsPath:String):DataFrame

  def generateSparkSchema()
  def readXML()
  def readParquet()
  def readAvro()
  def readORC()

  => One read function
  => One write function
  => xml with tag parameters
  => Convert a lot of files into one
  repartition, schema, format, mode , compression, save into table
  xml,
  => Generate a schema

  val spark: SparkSession


}

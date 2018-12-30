package com.demo.app
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.io.Source._
import org.json4s.jackson.JsonMethods.parse
import org.apache.spark.sql.Dataset;
import org.apache.log4j._
import org.apache.spark.sql.SparkSession;


/*
case class Location(
  `type`:String
  , coordinates:Array[String]
)
  

case class SensorData(
  bulletin_date:String
  , bulletin_date_formatted:String
  , easting:String
  , last_uploaded:String
  , latest_capture:String
  , latest_week:String
  , latitude:String
  , location:Location
  , longitude:String
  , no2_air_quality_band:String
  , no2_air_quality_index:String
  , northing:String
  , site_code:String
  , site_name:String
  , site_type:String
  , spatial_accuracy:String
  , ward_code:String
  , ward_name:String
)*/

case class CbseResult(
  region:String
  , total_appeared:String
  , total_passed:String
  , total_failed:String
  , percent_appeared:String
  , percent_passed:String
  , percent_failed:String
  )



object DemoApp {
  
  def main(args: Array[String]) {
    
    implicit val formats = org.json4s.DefaultFormats
    
    val spark = SparkSession.builder.master("local").appName("DemoApp").getOrCreate()
    import spark.implicits._
    
    //val parsedData = parse(fromURL("https://opendata.camden.gov.uk/resource/83f4-6in2.json?$limit=10").mkString).extract[Array[SensorData]]
    
    val cbseParsedData = parse(fromURL("http://localhost:3000/result").mkString).extract[Array[CbseResult]]
    
    val mySourceDataset = spark.createDataset(cbseParsedData)
    
    mySourceDataset.printSchema()
    
    mySourceDataset.createOrReplaceTempView("cbse_result_data")
    
    spark.sql("SELECT region, total_appeared, total_passed, total_failed FROM cbse_result_data").show
    
  
}
}
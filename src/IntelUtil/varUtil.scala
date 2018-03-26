package IntelUtil

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import scala.collection.mutable.HashMap		


object varUtil { 
  def main(args: Array[String]) { 
  }
		
  val startdate = "20160901"
  val enddate = "20160930"
  val rangeDir = "xrli/IntelDNN/Weika/201609_new/" 
  
 val testDir = "xrli/IntelDNN/Weika/Dir_test/" 
 //val testDir = "xrli/IntelDNN/Weika/Dir_test_all/"
  
  val idx_model = "xrli/IntelDNN/Weika/models/index_Model_0701"
			
}
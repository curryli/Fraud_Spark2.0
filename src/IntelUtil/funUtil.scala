package IntelUtil

import org.apache.log4j.Level
import org.apache.log4j.Logger
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
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import scala.collection.mutable.MutableList
import scala.Range
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{Buffer,Set,Map}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.PipelineModel
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable.HashMap

object funUtil { 
  
  def main(args: Array[String]) { 
    println(dayForWeek("20161001"))
  }
		 
	
  def getPipeline(DisperseArr: Array[String]): ArrayBuffer[PipelineStage] = {
    val pipelineStages = new ArrayBuffer[PipelineStage]
    var i=0
    for (col <- DisperseArr) {
      i=i+1
      println(i, col + "_idx")
      pipelineStages += new StringIndexer()
        .setInputCol(col + "_filled")
        .setOutputCol(col + "_CatVec")
        .setHandleInvalid("skip")
    }
    
    pipelineStages
  }
  
  
  def idx_Pipeline(DisperseArr: Array[String]): ArrayBuffer[PipelineStage] = {
    val pipelineStages = new ArrayBuffer[PipelineStage]
    var i=0
    for (col <- DisperseArr) {
      i=i+1
      println(i, col + "_idx")
      pipelineStages += new StringIndexer()
        .setInputCol(col)
        .setOutputCol(col + "_idx")
        .setHandleInvalid("skip")
    }
    
    pipelineStages
  } 
  
 def dayForWeek(pTime: String): Int={  
     //val format = new SimpleDateFormat("yyyy-MM-dd")
     val format = new SimpleDateFormat("yyyyMMdd")
     var c = Calendar.getInstance()
     c.setTime(format.parse(pTime)) 
     var dayForWeek = 0;  
     if(c.get(Calendar.DAY_OF_WEEK) == 1)
       dayForWeek = 7
     else  
      dayForWeek = c.get(Calendar.DAY_OF_WEEK) - 1
      
     return dayForWeek;  
 }
 
 
   
   def getDeltaTime(start: String, end: String, interval: String): Double={
		  val format = new SimpleDateFormat("MMddHHmmss")
			
		  try {
  		  var start_t = format.parse(start) 
  			var end_t = format.parse(end) 
  			val delta_ms = (end_t.getTime()-start_t.getTime()).toDouble
  			
  			val delta_seconds = delta_ms/1000
  			val delta_mins = delta_seconds/60
  			val delta_hours = delta_mins/60
  			 
  			var result =
         interval match {
           case "seconds"   => delta_seconds
           case "mins"   => delta_mins
           case "hours"    => delta_hours
        }
  			return result 
		  }
		  
		  catch {
         case ex: ParseException =>{
            println("ParseException")
            
          return Double.NaN
         }
		  }
	}
			
 
 def get_woe_map(vec_data:DataFrame, colname:String, label_col:String="label_filled"): HashMap[Double,Double]={
     var bad_all = vec_data.filter(vec_data(label_col)==="1.0") 
     var good_all = vec_data.filter(vec_data(label_col)==="0.0") 
     var bad_count_all = bad_all.count()
     var good_count_all = good_all.count()
     println("bad_count_all count is: " +  bad_count_all)
     println("good_count_all count is: " +  good_count_all)
      
     val woeMap = new HashMap[Double,Double]()
     
     //val QD_num = QD_money_num
     val cat_set = vec_data.select(colname).rdd.map(x=>x.getDouble(0)).distinct().collect
     println("cat_set is : ")
     cat_set.foreach{println}
     
     for(i<-cat_set){
        var bad_i = bad_all.filter(bad_all(colname)===i.toDouble) 
        var bad_count_i = bad_i.count()
        println("bad_count_i " + i +  " count is: " +  bad_count_i)   
        
        var good_i = good_all.filter(good_all(colname)===i.toDouble) 
        var good_count_i = good_i.count()
        println("good_count_i " + i +  " count is: " +  good_count_i)   
      
        var woe_i = Math.log((bad_count_i.toDouble/bad_count_all.toDouble)/(good_count_i.toDouble/good_count_all.toDouble))     
        //var woe_i = Math.log((bad_count_i/bad_count_all)*(good_count_all/good_count_i))
        
        woeMap.+=(i.toDouble -> woe_i)
     }
     
     woeMap 
  }
  
  def WOE_modify(vec_data:DataFrame, colname:String, woeMap: HashMap[Double, Double]):DataFrame={
    val map_name = "woeMap_" + colname
 
    val newcol_name = colname + "_WOE"
    val udf_woemap = udf[Double, Double]{key => woeMap(key)}
    
    val modified_vec = vec_data.withColumn(newcol_name, udf_woemap(vec_data(colname)))
    
    modified_vec
  }
  
  case class CF_Matrix(
    val TP_Cnt: Double,
    val TN_Cnt: Double,
    val FP_Cnt: Double,
    val FN_Cnt: Double,
    val Precision_P: Double,
    val Recall_P: Double
  ) 
    
  
  def get_CF_Matrix(predicted_DF: DataFrame): CF_Matrix={
     val TP_Cnt = predicted_DF.filter(predicted_DF("label_idx") === predicted_DF("prediction")).filter(predicted_DF("label_idx")===1).count.toDouble
     val TN_Cnt = predicted_DF.filter(predicted_DF("label_idx") === predicted_DF("prediction")).filter(predicted_DF("label_idx")===0).count.toDouble
     val FP_Cnt = predicted_DF.filter(predicted_DF("label_idx") !== predicted_DF("prediction")).filter(predicted_DF("prediction")===1).count.toDouble
     val FN_Cnt = predicted_DF.filter(predicted_DF("label_idx") !== predicted_DF("prediction")).filter(predicted_DF("prediction")===0).count.toDouble
     println("TP_Cnt is: " + TP_Cnt)
     println("TN_Cnt is: " + TN_Cnt)
     println("FP_Cnt is: " + FP_Cnt)
     println("FN_Cnt is: " + FN_Cnt)
     
     val Precision_P = TP_Cnt/(TP_Cnt + FP_Cnt)
     val Recall_P = TP_Cnt/(TP_Cnt + FN_Cnt)
     val result = new CF_Matrix(TP_Cnt,TN_Cnt,FP_Cnt,FN_Cnt,Precision_P,Recall_P)
     result
  }
  
  
  def Multi_idx_Pipeline(DisperseArr: Array[String]): ArrayBuffer[PipelineStage] = {
    val pipelineStages = new ArrayBuffer[PipelineStage]
    for (col <- DisperseArr) {
      pipelineStages += new StringIndexer()
        .setInputCol(col)
        .setOutputCol(col + "_idx")
        .setHandleInvalid("skip")
    }
    
    pipelineStages
  }
  
  def get_schema(arr: Array[String]): StructType ={
    var tmp_schema = new StructType()
    for(item<-arr){
      tmp_schema = tmp_schema.add(item,StringType,true)
    }
    tmp_schema
  }
  
  
//固定覆盖率    一系列密密麻麻的，选最接近的  
//  import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
//     val metrics = new BinaryClassificationMetrics(scoreAndLabels)
//     val v1=metrics.pr().filter(f=> f._1<=0.75).sortBy(f=>f._1, false, 1).first()
//     val v2=metrics.pr().filter(f=> f._1<=0.50).sortBy(f=>f._1, false, 1).first()
//     val v3=metrics.pr().filter(f=> f._1<=0.25).sortBy(f=>f._1, false, 1).first()

  
  
}
package GraphXDemo

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
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD 
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.lib._
import Algorithm._

object LPA_test {
  def main(args: Array[String]) {
 
   //屏蔽日志
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR); 
    
    //val sparkConf = new SparkConf().setAppName("spark2SQL")
    val warehouseLocation = "spark-warehouse"
    
    val ss = SparkSession
      .builder()
      .appName("Save_IndexerPipeLine")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.schema.verification", false)
      .getOrCreate()
  
    import ss.implicits._
    
    
//    val rdd = sc.makeRDD(Array("1 2","1 3","2 4","3 4","3 5","4 5","5 6","6 7","6 9","7 11","7 8","9 8","9 13","8 10","10 13","13 12","10 11","11 12"))
//    val edge =rdd .map( line =>{
//      val pair = line.split(" ")
//      Edge( pair(0).toLong,pair(1).toLong,1L )
//      })
//     
//    val graph = Graph.fromEdges( edge,0 )
////     
    
    
    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(ss.sparkContext, "xrli/graphx/testedges2.txt").cache
    
 //ERROR scheduler.LiveListenerBus: SparkListenerBus has already stopped! Dropping event SparkListenerExecutorMetricsUpdate(10,WrappedArray())  不知道是什么错， 但不影响结果
    //估计是我只在main 函数的最后   sc.stop  可能后续版本会修复

    var startTime = System.currentTimeMillis(); 
    
    val LPAgraph = LabelPropagation.run(graph, 200)    
    println("Show LPAgraph:")
    LPAgraph.vertices.collect().foreach(println) 
    LPAgraph.unpersistVertices(blocking = false)
    LPAgraph.edges.unpersist(blocking = false)
    println("Origin LPA done in " + (System.currentTimeMillis()-startTime))   
    
    
//    
    startTime = System.currentTimeMillis(); 
    val cgraph = LPA.run(ss,graph, 200)    
    println("Show cgraph:")
    cgraph.vertices.collect().foreach(println) 
    cgraph.unpersistVertices(blocking = false)
    cgraph.edges.unpersist(blocking = false)
    println("normal LPA done in " + (System.currentTimeMillis()-startTime))   
     
    startTime = System.currentTimeMillis(); 
    val perfgraphConv = myLPA.runConverg_last2(graph, 200)
    println("Show perfgraphConv:")
    perfgraphConv.vertices.collect().foreach(println) 
    perfgraphConv.unpersistVertices(blocking = false)
    perfgraphConv.edges.unpersist(blocking = false)
    println("runConverg_last2 done in " + (System.currentTimeMillis()-startTime))      
    
    startTime = System.currentTimeMillis(); 
    val mycgraph = myLPA.run(graph, 200)
    println("Show mycgraph:")
    mycgraph.vertices.collect().foreach(println) 
    mycgraph.unpersistVertices(blocking = false)
    mycgraph.edges.unpersist(blocking = false)
    println("normal myLPA done in " + (System.currentTimeMillis()-startTime))   
    
    startTime = System.currentTimeMillis(); 
    val mycgraphConv = myLPA.runUntilConvergence(graph, 200)
    println("Show mycgraphConv:")
    mycgraphConv.vertices.collect().foreach(println) 
    mycgraphConv.unpersistVertices(blocking = false)
    mycgraphConv.edges.unpersist(blocking = false)
    println("runUntilConvergence done in " + (System.currentTimeMillis()-startTime))   

    ss.stop();
  
  }
}
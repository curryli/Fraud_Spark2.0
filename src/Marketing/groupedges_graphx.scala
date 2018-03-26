package Marketing


import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import scala.collection.immutable.SortedSet
import scala.collection.mutable.ArrayBuffer

import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.Calendar

import java.util.Date
import org.apache.spark.mllib.fpm.PrefixSpan
import scala.collection.mutable.ArrayBuffer
import Algorithm._
import SparkContext._ 
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import scala.reflect.ClassTag

object groupedges_graphx {
  def main (args: Array[String]){

    val conf = new SparkConf()
    conf.setAppName("AprioriSeq")
    //conf.setMaster("local")

    val sc = new SparkContext(conf)

    val m_ratio = 0.2
    val out_dir = "xrli/FreqItem/APPout/"
   
    //val lines = sc.textFile("xrli/CardholderTag/traffic_sh/")
    
    val lines = sc.textFile("hdfs://nameservice1/user/hive/warehouse/00012900_shanghai.db/xrli_test_v2")
    
    
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      
    
    //0 pri_acct_no_conv	1 mchnt_cd	2 ls_mchnt_cd	3 time_diff
    var InPairRdd = lines.map(line=>{
	   val arr = line.split("\\001")
	   (HashEncode.HashMD5(arr(1)), arr(1))}
	  )
	  
    var OutPairRdd = lines.map(line=>{
	   val arr = line.split("\\001")
	   (HashEncode.HashMD5(arr(2)), arr(2))}
	  )

    val verticeRDD = InPairRdd.union(OutPairRdd).distinct()
    
    val edgeRDD = lines.map { line=>
        val arr = line.split("\\001")
        val srcId = HashEncode.HashMD5(arr(1))
        val dstId = HashEncode.HashMD5(arr(2))
        val card = arr(0)
        val diff = arr(3)
        Edge(srcId, dstId, (1, card, diff))
    } 
    
    var origraph = Graph(verticeRDD, edgeRDD).partitionBy(PartitionStrategy.RandomVertexCut)    //必须在调用groupEdges之前调用Graph.partitionBy 。
    
    
    def concat_str(ea:String, eb:String):String = {
       ea+ "_" + eb
    }
    
    var graph = origraph.groupEdges((ea,eb) => (ea._1+eb._1, concat_str(ea._2,eb._2), ea._3+eb._3))    //每条边是  两个商户之间的关系（总次数，distinct卡号，总间隔时间）
    //graph.edges.take(100).foreach(println)
    
    //var graph = origraph.groupEdges((ea,eb) => (ea._1+eb._1, ea._2+ "_" + eb._2, ea._3+eb._3))    //每条边是  两个商户之间的关系（总次数，distinct卡号，总间隔时间）
 
     
    var graph_w = graph.mapEdges(f=>{
      val tot_cnt = f.attr._1.toDouble
      val distinct_card = f.attr._2.split("_").toSet.size
      val avg_diff = f.attr._3.toDouble/tot_cnt
      (tot_cnt, distinct_card, avg_diff)
     }
    )
    
    graph_w = graph_w.subgraph(epred = triplet => triplet.attr._1 > 2) 
     
    graph_w.edges.take(100).foreach(println)
    
    val e_cnt = graph_w.numEdges
 
    
    
    
    
//    val tempDegGraph = graph.outerJoinVertices(graph.degrees){
//      (vid, encard, DegOpt) => (encard, DegOpt.getOrElse(0))
//    }
//     
//     //去除边出入度和为2的图
//    var coregraph = tempDegGraph.subgraph(epred = triplet => (triplet.srcAttr._2 + triplet.dstAttr._2) > 2) 
// 
//    coregraph = coregraph.outerJoinVertices(coregraph.degrees){
//       (vid, tempProperty, degOpt) => (tempProperty._1, degOpt.getOrElse(0))
//    } 
//    
//    //去除度为0的点
//    val degGraph = coregraph.subgraph(vpred = (vid, property) => property._2!=0).cache
//    
    
    
    
    }
	   
  }
  
 

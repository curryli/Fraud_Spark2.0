package Marketing


import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import scala.collection.immutable.SortedSet
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Set

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
import Algorithm._
import Math.{min,max}

object card_mchnt_minmax {
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
    
    var eRDD = lines.map { line=>
        val arr = line.split("\\001")
        val srcId = HashEncode.HashMD5(arr(1))
        val dstId = HashEncode.HashMD5(arr(2))
        val card = arr(0)
        val diff = arr(3)
        ((srcId, dstId), (1, card, diff))
    } 
    
    
    //statRDD.reduceByKey((x,y)=>(x._1+y._1, x._2+y._2, x._3+y._3))
    
    
    
  val createCombiner = ( (v : (Int, String, String)) => (v._1, Set[String](v._2), v._3.toDouble) )  
	
	val mergeValue =  ( c : (Int, Set[String], Double), v : (Int, String, String) ) => 
		{
			 (c._1+1, c._2.+(v._2), c._3+v._3.toDouble)
		}

	val mergeCombiners =  (c1:  (Int, Set[String], Double), c2:  (Int, Set[String], Double)) => 
		{
			 (c1._1+c2._1, c1._2.++(c2._2), c1._3+c2._3)
		}
	
	val statRDD = eRDD.combineByKey(  
		createCombiner,
		mergeValue,
		mergeCombiners
	).map{case(k, v) =>{
	  val totcnt = v._1
	  val distcard = v._2.size
	  var timeweight = 0.0
	  if(v._3!=0)
	    timeweight = v._1/v._3
	  else
	    timeweight = 10.0 
	    (k, (totcnt, distcard, timeweight))
	  }
	}  //(总次数totcnt， 不同卡号数 distcard， 平均时间差 avgdiff 的倒数   )
    
	
	
	val max_totcnt = statRDD.map(x=>x._2._1).reduce(max)
	val min_distcard = statRDD.map(x=>x._2._2).reduce(min)
	val max_distcard = statRDD.map(x=>x._2._2).reduce(max)
	val min_timeweight = statRDD.map(x=>x._2._3).reduce(min)
	val max_timeweight = statRDD.map(x=>x._2._3).reduce(max)
	
	println(max_totcnt, min_distcard, max_distcard, min_timeweight, max_timeweight)
	
	
	//(x - min)/(max - min)
	def NormInt(x:Int, Min:Int, Max:Int):Double ={
	  (x - Min).toDouble/(Max - Min).toDouble
	}
	
  def NormDouble(x:Double, Min:Double, Max:Double):Double ={
	  (x - Min)/(Max - Min)
	}
 
 
  //map类型改变了，不能赋值给原来的statRDD  ，否则报错
  var NormRDD = statRDD.map{case(k, v) => (k, (NormInt(v._1, 1, max_totcnt) , NormInt(v._2, min_distcard, max_distcard), NormDouble(v._3, min_timeweight, max_timeweight)))} 
  NormRDD = NormRDD.filter(_._2._1>0)
  println("NormRDD count:" + NormRDD.count)
  NormRDD.take(100).foreach(println)
	
	val edgeRDD = NormRDD.map {f=>
        val srcId = f._1._1
        val dstId = f._1._2
        val weight = f._2._1 + f._2._2 + f._2._3
        Edge(srcId, dstId, weight)
  } 
	
	var origraph = Graph(verticeRDD, edgeRDD).partitionBy(PartitionStrategy.RandomVertexCut)    //必须在调用groupEdges之前调用Graph.partitionBy 。
     
  val tempDegGraph = origraph.outerJoinVertices(origraph.degrees){
      (vid, encard, DegOpt) => (encard, DegOpt.getOrElse(0))
    }
     
     //去除边出入度和为2的图
    var coregraph = tempDegGraph.subgraph(epred = triplet => (triplet.srcAttr._2 + triplet.dstAttr._2) > 2) 
 
    coregraph = coregraph.outerJoinVertices(coregraph.degrees){
       (vid, tempProperty, degOpt) => (tempProperty._1, degOpt.getOrElse(0))
    } 
    
    //去除度为0的点
    val degGraph = coregraph.subgraph(vpred = (vid, property) => property._2!=0)
	
		degGraph.edges.take(100).foreach(println)
	
	 
     
    //val Convgraph = simple_Pagerank.run_modify_edgeweight(degGraph, tol=0.001,numIter=1000, resetProb= 0.15)
    //val Convgraph = simple_Pagerank.runUntilConvergence(degGraph, tol=0.001,numIter=1000, resetProb= 0.15)
     val Convgraph = degGraph.pageRank(tol=0.001, resetProb= 0.15)
    
//    println("Show Convgraph:")
//    Convgraph.vertices.take(100).foreach(println)
    println("Show sorted Convgraph:")
    Convgraph.vertices.sortBy(x=>x._2, ascending=false).take(100).foreach(println)
    
     
    
    
    val Mcd_Mname = sc.textFile("hdfs://nameservice1/user/hive/warehouse/00012900_shanghai.db/mcd_mname")
    
    var Mcd_Mname_Rdd = Mcd_Mname.map(line=>{
	   val arr = line.split("\\001")
	   if( arr.length>1)
	     (HashEncode.HashMD5(arr(0)), arr(1)) 
     else
       (0L, " ")}
	  )
    
	  
	  
	  
	  val PRGraph = degGraph.outerJoinVertices(Convgraph.vertices){
       (vid, tempProperty, PROpt) => (tempProperty._1, PROpt.getOrElse(0.0))
    } 
	  
	  
	  
	  var PR_V_RDD = PRGraph.vertices.leftOuterJoin(Mcd_Mname_Rdd).map(x=> (x._2._2.getOrElse(" "), x._2._1._2))
	  
	  sc.parallelize(PR_V_RDD.sortBy(x=>x._2, ascending=false).take(500)).coalesce(1).saveAsTextFile("xrli/ValueAPI/MchntPagerank_ASC")
	  sc.parallelize(PR_V_RDD.sortBy(x=>x._2, ascending=true).take(500)).coalesce(1).saveAsTextFile("xrli/ValueAPI/MchntPagerank_DESC")
    
    
  }
  
}

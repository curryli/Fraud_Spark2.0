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

import Math.{min,max,sqrt}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

object card_mchnt_zscore {
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
	).map{case(k, v) => (k, (v._1, v._2.size, v._3/v._1))}   //(总次数totcnt， 不同卡号数 distcard， 平均时间差 avgdiff)
    
	
	val vecstat = statRDD.map(f => Vectors.dense(f._2._1, f._2._3, f._2._3))
	
	//https://yq.aliyun.com/articles/429728
	val summary: MultivariateStatisticalSummary = Statistics.colStats(vecstat)
	
  val mean_totcnt = summary.mean(0)
  val std_totcnt = sqrt(summary.variance(0))
  val mean_distcard = summary.mean(1)
  val std_distcard = sqrt(summary.variance(1))
  val mean_avgdiff = summary.mean(2)
  val std_avgdiff = sqrt(summary.variance(2))
  
 
	
	//(x - min)/(max - min)
	def NormInt(x:Int, Mean:Double, Std:Double):Double ={
	  (x - Mean).toDouble/Std
	}
	
  def NormDouble(x:Double, Mean:Double, Std:Double):Double ={
	  (x - Mean).toDouble/Std
	}
 
 
  //map类型改变了，不能赋值给原来的statRDD  ，否则报错
  val NormRDD = statRDD.map{case(k, v) => (k, (NormInt(v._1, mean_totcnt, std_totcnt) , NormInt(v._2, mean_distcard, std_distcard), NormDouble(v._2, mean_avgdiff, std_avgdiff)))} 
  NormRDD.take(100).foreach(println)
	
	val edgeRDD = NormRDD.map {f=>
        val srcId = f._1._1
        val dstId = f._1._2
        val weight = f._2._1 + f._2._2 + f._2._3
        Edge(srcId, dstId, weight)
  } 
	
	var origraph = Graph(verticeRDD, edgeRDD).partitionBy(PartitionStrategy.RandomVertexCut)    //必须在调用groupEdges之前调用Graph.partitionBy 。
    
	origraph.edges.take(100).foreach(println)
	
	
  }
  
}

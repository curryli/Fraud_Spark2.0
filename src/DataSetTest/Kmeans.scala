package DataSetTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.clustering.KMeans
 
 
object Kmeans {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    Logger.getLogger("hive").setLevel(Level.OFF);
    Logger.getLogger("parse").setLevel(Level.OFF);
    
    val sparkConf = new SparkConf().setAppName("Kmeans")
    val sc = new SparkContext(sparkConf)
    
    val sqlContext = new SQLContext(sc)
 
    // 装载数据集
    val oridata = sc.textFile("xrli/TeleFraud/testProp.csv")
    
    
      // The schema is encoded in a string
    //val schemaString = "ccLabel ccNum maxK maxInDeg maxOutDeg maxDeg BigKNum TransNum totalMoney totalTransCount foreignCount nightCount charge regionCount mchnttpCount mchntcdCount addrDetailCount"
    
    
    import sqlContext.implicits._
    val dataRDD = oridata.map(line => (line.split(',')(0).toLong, Vectors.dense( line.split(',').slice(1, 17).map(_.toDouble))))
    val dataFrame = dataRDD.toDF("ccLabel", "ccFeatures")
    println("Original dataframe:")
    dataFrame.show(5) 
      
    val normalizer2 = new Normalizer().setInputCol("ccFeatures").setOutputCol("normFeatures")     //默认是L2
    val l2NormData = normalizer2.transform(dataFrame)
    l2NormData.show(5)
    l2NormData.select("normFeatures").take(5).map { x => x.toSeq.foreach{println}}
    
    
    val kmeans = new KMeans().setK(2).setSeed(1L).setFeaturesCol("normFeatures").setPredictionCol("prediction") 
    val model = kmeans.fit(l2NormData)
     
    
    
    val WSSSE = model.computeCost(l2NormData.select("normFeatures"))
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
    
    val KmeansResult = model.transform(l2NormData)
     println("KmeansResult: ")
    KmeansResult.show(5)
    
    var df = KmeansResult.filter($"prediction"===1).select("ccLabel", "ccFeatures", "prediction")  
    //df不能直接保存为csv格式，因为"ccFeatures"是 Vectors.dense类型的。而df.write.csv函数只能保存数组每一列是int double 或者string这一类基本类型的dataframe
    
    df = df.withColumn("featureString", df("ccFeatures").cast(org.apache.spark.sql.types.StringType))
    
    println("newdf: ")
    df.show(5)
    
    df = df.select("ccLabel", "featureString", "prediction")
    df.write.csv("xrli/TeleFraud/testKmeansResult")
 
    sc.stop()
  }
}

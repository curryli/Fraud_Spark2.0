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
 
object testProcess {
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
    val oridata = sc.textFile("xrli/TeleFraud/testProp2.csv")
    
    
      // The schema is encoded in a string
    //val schemaString = "ccLabel ccNum maxK maxInDeg maxOutDeg maxDeg BigKNum TransNum totalMoney totalTransCount foreignCount nightCount charge regionCount mchnttpCount mchntcdCount addrDetailCount"
    
    
    import sqlContext.implicits._
    val dataRDD = oridata.map(line => (line.split(',')(0).toLong, Vectors.dense( line.split(',').slice(1, 17).map(_.toDouble))))
    val dataFrame = dataRDD.toDF("ccLabel", "ccFeatures")
    println("Original dataframe:")
    dataFrame.show(5) 
     
    val normalizer1 = new Normalizer().setInputCol("ccFeatures").setOutputCol("normFeatures").setP(1.0)
    val l1NormData = normalizer1.transform(dataFrame)
    l1NormData.show(5)
    
    val normalizer2 = new Normalizer().setInputCol("ccFeatures").setOutputCol("normFeatures")     //默认是L2
    val l2NormData = normalizer2.transform(dataFrame)
    l2NormData.show(5)
    
    val normalizer3 = new Normalizer().setInputCol("ccFeatures").setOutputCol("normFeatures")
    val l3NormData = normalizer3.transform(dataFrame, normalizer3.p -> Double.PositiveInfinity)
    l3NormData.show(5)
    
    
  
  
    val scaler = new StandardScaler()
     .setInputCol("ccFeatures")
     .setOutputCol("scaledFeatures")
     .setWithStd(true) //默认值为真，使用统一标准差方式。
     .setWithMean(false)  //withMean：默认为假。此种方法将产出一个稠密输出，所以不适用于稀疏输入。

    // Compute summary statistics by fitting the StandardScaler.
   val scalerModel = scaler.fit(dataFrame)
   // Normalize each feature to have unit standard deviation.
   val scaledData = scalerModel.transform(dataFrame)
   scaledData.show(5)

     val scaler2 = new MinMaxScaler()
     .setInputCol("features")
     .setOutputCol("scaledFeatures")
 
      
    sc.stop()
  }
}

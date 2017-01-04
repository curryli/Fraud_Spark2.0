package DataSetTest

import scala.math._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}


 
case class Person(name: String, age: Long)

object testDataSet { 
  def main(args: Array[String]) { 
     //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    
    val ss = SparkSession.builder().appName("DataSet basic example").appName("Dataset example").getOrCreate()
 
   // For implicit conversions like converting RDDs to DataFrames
    import ss.implicits._

    
///////////////////dataframe/////////////////////    
    val dataList: List[(Double, String, Double, Double, String, Double, Double, Double, Double)] = List(
      (0, "male", 37, 10, "no", 3, 18, 7, 4),
      (0, "female", 27, 4, "no", 4, 14, 6, 4),
      (0, "female", 32, 15, "yes", 1, 12, 1, 4),
      (0, "male", 57, 15, "yes", 5, 18, 6, 5),
      (0, "male", 22, 0.75, "no", 2, 17, 6, 3),
      (0, "female", 32, 1.5, "no", 2, 17, 5, 5),
      (0, "female", 22, 0.75, "no", 2, 12, 1, 3),
      (0, "male", 57, 15, "yes", 2, 14, 4, 4),
      (0, "female", 32, 15, "yes", 4, 16, 1, 2),
      (0, "male", 22, 1.5, "no", 4, 14, 4, 5))
 
    val DFdata = dataList.toDF("affairs", "gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")
 
    DFdata.printSchema()
    DFdata.show(7)
    val data3=DFdata.limit(5)
    DFdata.filter("age>50 and gender=='male' ").show
    val columnArray=DFdata.columns
    DFdata.select("gender", "age", "yearsmarried", "children").show(3)
    val colArray=Array("gender", "age", "yearsmarried", "children")
    DFdata.selectExpr(colArray:_*).show(3)
    DFdata.selectExpr("gender", "age+1 as age1","cast(age as bigint) as age2").sort($"gender".desc, $"age".asc).show

///////////////////dataset/////////////////////       
    ///方式1原生
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)
    
    // 方式2：自定义类    Encoders are created for case classes // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,  you can use custom classes that implement the Product interface
    val caseClassDS = Seq(Person("Andy", 32), Person("Louis", 33)).toDS()
    caseClassDS.show()
    
    //读文件
    val data = ss.read.text("xrli/TeleTrans/DStest.txt").as[String]
    val words = data.flatMap(value => value.split("\\s+"))
    val groupedWords = words.groupByKey(value=>value)
    val counts = groupedWords.count()
    counts.show()
    
  }
} 
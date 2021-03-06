package DataSetTest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
 
 
object TestVectorSlicer extends App {
   //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    Logger.getLogger("hive").setLevel(Level.OFF);
    Logger.getLogger("parse").setLevel(Level.OFF);
    
    val sparkConf = new SparkConf().setAppName("transGraph")
    val sc = new SparkContext(sparkConf)
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    
    //构造特征数组
    val data = Array(Row(Vectors.dense(-2.0, 2.3, 0.0)))
    
    //为特征数组设置属性名（字段名），分别为f1 f2 f3
    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])
    
    //构造DataFrame
    val dataRDD = sc.parallelize(data)
    val dataset = sqlContext.createDataFrame(dataRDD, StructType(Array(attrGroup.toStructField())))
    
    print("原始特征：")
    dataset.take(1).foreach(println)
    
    
    //构造切割器
    var slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")
    
    //根据索引号，截取原始特征向量的第1列和第3列
    slicer.setIndices(Array(0,2))
    print("output1: ") 
    slicer.transform(dataset).select("userFeatures", "features").first()
    
    //根据字段名，截取原始特征向量的f2和f3
    slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")
    slicer.setNames(Array("f2","f3"))
    print("output2: ") 
    slicer.transform(dataset).select("userFeatures", "features").first()
    
    //索引号和字段名也可以组合使用，截取原始特征向量的第1列和f2
    slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")
    slicer.setIndices(Array(0)).setNames(Array("f2"))
    print("output3: ") 
    slicer.transform(dataset).select("userFeatures", "features").first()
    
    
}
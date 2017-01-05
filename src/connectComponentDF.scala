 
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
 

 

object connectComponentDF {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("SimpleGraphX") 
    val sc = new SparkContext(conf)
    
    val warehouseLocation = "spark-warehouse"
    val ss = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.schema.verification", false)
      .enableHiveSupport()
      .getOrCreate()
      
      
 
    //设置顶点和边，注意顶点和边都是用元组定义的Array
    //顶点的数据类型是VD:(String,Int)
    val vertexArray = Array(
      (1L, "a"),
      (2L, "b"),
      (3L, "c"),
      (4L, "d"),
      (5L, "e"),
      (6L, "f"),
      (7L, "g"),
      (8L, "h")
    )
    //边的数据类型ED:Int
    val edgeArray = Array(
      Edge(1L, 2L, 99),
      Edge(2L, 3L, 99),
      Edge(3L, 1L, 99),
      Edge(2L, 4L, 99),
      Edge(4L, 5L, 99),
      Edge(5L, 2L, 99),
      Edge(5L, 6L, 99),
      Edge(3L, 6L, 99),
      Edge(7L, 8L, 99)
    )
 
    //构造vertexRDD和edgeRDD
    val vertexRDD = sc.parallelize(vertexArray)
    val edgeRDD = sc.parallelize(edgeArray)
 
    //构造图Graph[VD,ED]
    var graph = Graph(vertexRDD, edgeRDD)
 
//    var cgraph = graph.connectedComponents()  //可以认为是针对无向图，只要有边即可算连在一起
//    cgraph.vertices.collect().foreach(println)
//    //cgraph.edges.collect().foreach(println)   //边还是和原来graph一模一样
//    
//    val maxIterations=10   //the maximum number of iterations to run for
//    var sgraph = graph.stronglyConnectedComponents(maxIterations)   //针对有向图，每个强连通图内部的节点到其他节点必须有路径到达
//    sgraph.vertices.collect().foreach(println)
//    
//    val connectedCount = cgraph.vertices.map(pair=>(pair._2, 1)).reduceByKey(_+_).sortBy(f => f._2, true)
//    println(connectedCount.collect().mkString("\n"))
//    println(connectedCount.count)
    
    
/********************测试结果*********************************   
cgraph.vertices.collect().foreach(println)        (7,7) (8,7)代表原来graph图中的顶点7和8属于同一个connectedComponent，该connectedComponent代号用其中最小的顶点7表示
(1,1)
(2,1)
(3,1)
(4,1)
(5,1)
(6,1)
(7,7)
(8,7)

sgraph.vertices.collect().foreach(println)         顶点7 8是单向孤岛，  顶点8没有有向边能够到达顶点7  因此顶点7 8 不属于同一个强连通分量。
(1,1)
(2,1)
(3,1)
(4,1)
(5,1)
(6,6)
(7,7)
(8,8)
 
println(connectedCount.collect().mkString("\n"))
(7,2)    对应7连通图的有2个点
(1,6)   对应1连通图的有6个点

println(connectedCount.count)
2
 
*******************测试结果*********************************/      
    
    
    
    val degGraph = graph.outerJoinVertices(graph.degrees){
      (vid, name, DegOpt) => (name, DegOpt.getOrElse(0))
    }
     
     //去除边出入度和为2的图
    var tempgraph = degGraph.subgraph(triplet => (triplet.srcAttr._2 + triplet.dstAttr._2) > 2)

    tempgraph.vertices.collect().foreach(println)
    
    val maxIterations=10   //the maximum number of iterations to run for
    var cgraph = tempgraph.stronglyConnectedComponents(maxIterations)
    
    val ccgraph = tempgraph.outerJoinVertices(cgraph.vertices){
      (vid, tempProperty, connectedOpt) => (tempProperty._1, connectedOpt)
    }
    // ccgraph   (vid,顶点名称，对应团体)
     
    
 
    
//    import ss.implicits._
//    val cCDF = connectedCount.toDF()
//    val cVDF = ccgraph.vertices.toDF()
    
    val connectedCount = cgraph.vertices.map(pair=>(pair._2, 1)).reduceByKey(_+_).sortBy(f => f._2, true)
    //connectedCount  (团体, 对应团体规模) 
    
    val ccVertice = ccgraph.vertices.map(line => (line._1, line._2._1, line._2._2.get))
    
 
    import ss.implicits._
    val cCDF = connectedCount.toDF("cc","count")
    val cVDF = ccVertice.toDF("vid","name","cc")
    
    var joinedDF = cVDF.join(cCDF, cVDF("cc") === cCDF("cc"), "left_outer").drop(cVDF("cc"))
    joinedDF.show()

//不drop
//+---+----+---+---+-----+
//|vid|name| cc| cc|count|
//+---+----+---+---+-----+
//|  7|   g|  7|  7|    1|
//|  6|   f|  6|  6|    1|
//|  5|   e|  1|  1|    5|
//|  2|   b|  1|  1|    5|
//|  3|   c|  1|  1|    5|
//|  4|   d|  1|  1|    5|
//|  1|   a|  1|  1|    5|
//|  8|   h|  8|  8|    1|
//+---+----+---+---+-----+
    
//drop
//+---+----+---+-----+
//|vid|name| cc|count|
//+---+----+---+-----+
//|  7|   g|  7|    1|
//|  6|   f|  6|    1|
//|  5|   e|  1|    5|
//|  2|   b|  1|    5|
//|  4|   d|  1|    5|
//|  3|   c|  1|    5|
//|  1|   a|  1|    5|
//|  8|   h|  8|    1|
//+---+----+---+-----+

    
     val VidconnectedCount = joinedDF.map(row=>(row.getLong(0), (row.getString(1),row.getLong(2),row.getInt(3)))).rdd

    //VidconnectedCount    RDD(Vid, (顶点名称，对应团体, 对应团体规模)) 

    val cCountgraph = graph.outerJoinVertices(VidconnectedCount){
      (vid, oldProperty, vccCountprop) => vccCountprop.get
    }
     // cCountgraph 的顶点属性为  (vid,顶点名称，对应团体, 对应团体规模)
    
    cCountgraph.vertices.collect().foreach(println) 
    

//(vid,(顶点名称，对应团体, 对应团体规模))
//(1,(a,1,5))
//(2,(b,1,5))
//(3,(c,1,5))
//(4,(d,1,5))
//(5,(e,1,5))
//(6,(f,6,1))
//(7,(g,7,1))
//(8,(h,8,1))
    
    sc.stop()
  }
}



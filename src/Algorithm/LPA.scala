package Algorithm
//这边其实没有加收敛条件。需要全局判断当前标签集合与上上步的标签集合的差别是否小于一个阈值。  上上步是为了防止二部震荡
import scala.reflect.ClassTag
import org.apache.spark.graphx._
import scala.Iterator
import org.apache.spark.sql.SparkSession

/** Label Propagation algorithm. */
object LPA {
 
  def run[VD, ED: ClassTag](ss: SparkSession, graph: Graph[VD, ED], maxSteps: Int=100): Graph[VertexId, ED] = {
    require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got ${maxSteps}")

    val lpaGraph = graph.mapVertices { case (vid, _) => vid }  //初始化图定点属性，即LPA的标签，开始时每个顶点的标签为顶点id
    
    val initialMessage = Map[VertexId, Long]()
  
    //即消息发送函数，给所有相邻节点发送该节点的attr(即顶点label)之用，双向，从源顶点<---->目标定点
    //原始的LPA算法的迭代停止条件应该是        判断当前所有标签和上上轮总标签的差别，如果差别很小则停止  （不用上轮是防止二部震荡）  这个需要需要全局判断，要在原始的pregel中修改源码。    
 
    def sendMessage(e: EdgeTriplet[VertexId, ED]): Iterator[(VertexId, Map[VertexId, Long])] = {
        Iterator((e.srcId, Map(e.dstAttr -> 1L)), (e.dstId, Map(e.srcAttr -> 1L))) 
    }
    
    //消息合并函数，对发送而来的消息进行merge，原理：对发送而来的Map，取Key进行合并，并对相同key的值进行累加操作
    def mergeMessage(count1: Map[VertexId, Long], count2: Map[VertexId, Long])
      : Map[VertexId, Long] = {
      (count1.keySet ++ count2.keySet).map { i =>
        val count1Val = count1.getOrElse(i, 0L)
        val count2Val = count2.getOrElse(i, 0L)
        i -> (count1Val + count2Val)
      }.toMap  //  (collection.breakOut)  more efficient alternative to [[collection.Traversable.toMap]]      也就是说 (collection.breakOut) 是.toMap 的升级版
    }
    
    //该函数用于在完成一次迭代的时候，将第一次的结果和原图做关联
    //顶点函数，若消息为空，则保持不变，否则取消息中数量最多的标签，即Map中value最大的key。
    def vertexProgram(vid: VertexId, attr: Long, message: Map[VertexId, Long]): VertexId = {
      if (message.isEmpty) attr else message.maxBy(_._2)._1
    }
     
 
     val pregelGraph = Pregel(lpaGraph, initialMessage, maxIterations = maxSteps)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)
     
     lpaGraph.unpersist(blocking=false)
      
    pregelGraph
  }
}
package Algorithm

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import scala.Iterator
import scala.collection.immutable.Map

/** Label Propagation algorithm. */
object myLPA {
 
  def run[VD, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int=100): Graph[VertexId, ED] = {
    require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got ${maxSteps}")

    val lpaGraph = graph.mapVertices { case (vid, _) => vid }  //初始化图定点属性，即LPA的标签，开始时每个顶点的标签为顶点id
     
    //即消息发送函数，给所有相邻节点发送该节点的attr(即顶点label)之用，双向，从源顶点<---->目标定点
    def sendMessage(e: EdgeTriplet[VertexId, ED]): Iterator[(VertexId, Map[VertexId, Long])] = {
      Iterator((e.srcId, Map(e.dstAttr -> 1L)), (e.dstId, Map(e.srcAttr -> 1L)))
    }
    
    //    这里的map只能是不可变map
    //消息合并函数，对发送而来的消息进行merge，原理：对发送而来的Map，取Key进行合并，并对相同key的值进行累加操作
    def mergeMessage(count1: Map[VertexId, Long], count2: Map[VertexId, Long]) : Map[VertexId, Long] = {
      val merge_keys = count1.keySet ++ count2.keySet
      var merge_map:Map[VertexId, Long] = Map[VertexId, Long]()
      for(key<-merge_keys){
        val count1Val = count1.getOrElse(key, 0L)
        val count2Val = count2.getOrElse(key, 0L)
        val k_cnt = count1Val+count2Val
        merge_map.+=(key->k_cnt)
      }
      
      merge_map
    }

    //该函数用于在完成一次迭代的时候，将第一次的结果和原图做关联
    //顶点函数，若消息为空，则保持不变，否则取消息中数量最多的标签，即Map中value最大的key。
    def vertexProgram(vid: VertexId, attr: Long, message: Map[VertexId, Long]): VertexId = {
      if (message.isEmpty) attr else message.maxBy(_._2)._1
    }
     
 
     val pregelGraph = Pregel(lpaGraph, Map[VertexId, Long](), maxIterations = maxSteps, EdgeDirection.Either)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)
     
     lpaGraph.unpersistVertices(blocking = false)
     lpaGraph.edges.unpersist(blocking = false)
     
    pregelGraph
  }
   
  
  //增加终止条件
  def runUntilConvergence[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int=100) =
  {
 
    val lpaGraph = graph.mapVertices { case (vid, _) => (vid, 0L)}.cache()  //初始化图定点属性: (vid, 上轮社区标签（初始化为0）) 
 
        //即消息发送函数，给所有相邻节点发送该节点的attr(即顶点label)之用，双向，从源顶点<---->目标定点
    def sendMessage(e: EdgeTriplet[(VertexId,Long), ED]) = {
       if ((e.srcAttr._1 != e.srcAttr._2) & (e.dstAttr._1 != e.dstAttr._2)){
        Iterator((e.srcId, Map(e.dstAttr._1 -> 1L)), (e.dstId, Map(e.srcAttr._1 -> 1L)))
      } 
       else if (e.srcAttr._1 != e.srcAttr._2){
          Iterator((e.dstId, Map(e.srcAttr._1 -> 1L)))
       }
       else if (e.dstAttr._1 != e.dstAttr._2){
           Iterator((e.srcId, Map(e.dstAttr._1 -> 1L)))
       }
       else{
        Iterator.empty
      }
    }
    
     
     //消息合并函数，对发送而来的消息进行merge，原理：对发送而来的Map，取Key进行合并，并对相同key的值进行累加操作
    def mergeMessage(count1: Map[VertexId, Long], count2: Map[VertexId, Long]) = {
      (count1.keySet ++ count2.keySet).map { i =>
        val count1Val = count1.getOrElse(i, 0L)
        val count2Val = count2.getOrElse(i, 0L)
        i -> (count1Val + count2Val)
      }.toMap  //  (collection.breakOut)  more efficient alternative to [[collection.Traversable.toMap]]      也就是说 (collection.breakOut) 是.toMap 的升级版
    }

    //该函数用于在完成一次迭代的时候，将第一次的结果和原图做关联
    //顶点函数，若消息为空，则保持不变，否则取消息中数量最多的标签，即Map中value最大的key。
    def vertexProgram(vid: VertexId, attr: (VertexId,Long), message: Map[VertexId, Long]) = {
      if (message.isEmpty) attr 
      else{
        val newCCid = message.maxBy(_._2)._1  //发送过来数量最多的对应的社团
        val lastCCid = attr._1
        (newCCid,lastCCid)
      }
    }
     
    val initialMessage = Map[VertexId, Long]()
 
     val pregelGraph = Pregel(lpaGraph, initialMessage, maxIterations = maxSteps, EdgeDirection.Either)(
      vertexProgram, sendMessage, mergeMessage).mapVertices((vid, attr) => attr._1)
     
     lpaGraph.unpersistVertices(blocking = false)
     lpaGraph.edges.unpersist(blocking = false)
      
    pregelGraph
  }

  
   
  
  //其实为了防止二部震荡，最好拿当前社团属性与上上次 的社团属性取比较，这样 每个节点的属性就定义为  (新接收到的社团, 上一次的社团，上上次的社团)
  //其实这个方法也很难收敛，最好的是全局判断，全局判断要在pregel源码里面修改
  def runConverg_last2[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int=100) =
  {
 
    val lpaGraph = graph.mapVertices { case (vid, _) => (vid, 0L, 0L)}.cache()  //初始化图定点属性: (vid, 上轮社区标签（初始化为0）) 
 
        //即消息发送函数，给所有相邻节点发送该节点的attr(即顶点label)之用，双向，从源顶点<---->目标定点
    def sendMessage(e: EdgeTriplet[(VertexId,Long,Long), ED]) = {
       if ((e.srcAttr._1 != e.srcAttr._3) & (e.dstAttr._1 != e.dstAttr._3)){
        Iterator((e.srcId, Map(e.dstAttr._1 -> 1L)), (e.dstId, Map(e.srcAttr._1 -> 1L)))
      } 
       else if (e.srcAttr._1 != e.srcAttr._3){
          Iterator((e.dstId, Map(e.srcAttr._1 -> 1L)))
       }
       else if (e.dstAttr._1 != e.dstAttr._3){
           Iterator((e.srcId, Map(e.dstAttr._1 -> 1L)))
       }
       else{
        Iterator.empty
      }
    }
    
     
     //消息合并函数，对发送而来的消息进行merge，原理：对发送而来的Map，取Key进行合并，并对相同key的值进行累加操作
    def mergeMessage(count1: Map[VertexId, Long], count2: Map[VertexId, Long]) = {
      (count1.keySet ++ count2.keySet).map { i =>
        val count1Val = count1.getOrElse(i, 0L)
        val count2Val = count2.getOrElse(i, 0L)
        i -> (count1Val + count2Val)
      }.toMap  //  (collection.breakOut)  more efficient alternative to [[collection.Traversable.toMap]]      也就是说 (collection.breakOut) 是.toMap 的升级版
    }

    //该函数用于在完成一次迭代的时候，将第一次的结果和原图做关联
    //顶点函数，若消息为空，则保持不变，否则取消息中数量最多的标签，即Map中value最大的key。
    def vertexProgram(vid: VertexId, attr: (VertexId,Long,Long), message: Map[VertexId, Long]) = {
      if (message.isEmpty) attr 
      else{
        val newCCid = message.maxBy(_._2)._1  //发送过来数量最多的对应的社团
        val lastCCid = attr._1
        val last2CCid = attr._2
        (newCCid,lastCCid,last2CCid)
      }
    }
     
    val initialMessage = Map[VertexId, Long]()
 
     val pregelGraph = Pregel(lpaGraph, initialMessage, maxIterations = maxSteps, EdgeDirection.Either)(
      vertexProgram, sendMessage, mergeMessage).mapVertices((vid, attr) => attr._1)
     
     lpaGraph.unpersistVertices(blocking = false)
     lpaGraph.edges.unpersist(blocking = false)
      
    pregelGraph
  }
  
   
  
}
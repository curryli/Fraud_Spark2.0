import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession




object transGraph { 
  def main(args: Array[String]) { 
     //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
//    val sparkConf = new SparkConf().setAppName("transGraph")
//    val sc = new SparkContext(sparkConf)
    
    val warehouseLocation = "spark-warehouse"
    val ss = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.schema.verification", false)
      .enableHiveSupport()
      .getOrCreate()
  
    import ss.implicits._
    import ss.sql
    
    sql(s"set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat" +
           s"set mapred.max.split.size=10240000000" +
           s"set mapred.min.split.size.per.node=10240000000" +
           s"set mapred.min.split.size.per.rack=10240000000" +
           s"set mapreduce.jobtracker.split.metainfo.maxsize = -1" +
           s"set mapreduce.job.queuename=root.queue2")

    sql("CREATE DATABASE IF NOT EXISTS TeleFraud")
    sql("USE TeleFraud")
    sql(s"CREATE TABLE IF NOT EXISTS tele_trans0701(" + 
                    s"tfr_in_acct_no string," + 
                    s"tfr_out_acct_no string," + 
                    s"fwd_settle_at double," + 
                    s"hp_settle_dt string," + 
                    s"loc_trans_tm string," + 
                    s"acpt_ins_id_cd string," + 
                    s"trans_md string," + 
                    s"cross_dist_in string)")
                    
     
    val data = sql(
      s"select tfr_in_acct_no," +
      s"tfr_out_acct_no, " +
      s"fwd_settle_at, " +
      s"hp_settle_dt, " +
      s"loc_trans_tm, " +
      s"acpt_ins_id_cd, " +
      s"trans_md, " +
      s"cross_dist_in " +
      s"from hbkdb.dtdtrs_dlt_cups where " +
      s"hp_settle_dt>=20160701 and hp_settle_dt<=20160702 " +
      s"and trans_id ='S33' ").toDF("srccard","dstcard","money","date","time","acpt_ins_id_cd","trans_md","cross_dist_in").repartition(100)
        
      val transdata = data.groupBy("srccard","dstcard").sum("money")
      transdata.show()
      
// +--------------------+--------------------+----------+
//|             srccard|             dstcard|sum(money)|
//+--------------------+--------------------+----------+
//|7b04389fcd2749448...|795a56aedc7a97baa...|     20000|
//|f7bb43d3faeee9779...|f5e2334ad37a10dd2...|    700000|
//|2338c9b1aefee17a1...|8e601f99a4c71c5ba...|    250000|
//|8d0713493d3ae050a...|fc522b2382d940d2d...|     60600|
      
      
    val InPairRdd = transdata.map(line => (BKDRHash(line.getString(0)), line.getString(0)))                
    val OutPairRdd = transdata.map(line => (BKDRHash(line.getString(1)), line.getString(1)))      
    val verticeRDD = InPairRdd.union(OutPairRdd).distinct()
    
    println(verticeRDD.count())
    println(verticeRDD.take(5))
    
 
    
    
  } 
  
  
  
  def BKDRHash( str:String) :Long ={
   val seed:Long  = 131 // 31 131 1313 13131 131313 etc..
   var hash:Long  = 0
   for(i <- 0 to str.length-1){
    hash = hash * seed + str.charAt(i)
    hash = hash.&("137438953471".toLong)        //0x1FFFFFFFFF              //固定一下长度
   }
   return hash 
}
  
} 
package IntelUtil
 
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._


object get_from_HDFS { 
  def main(args: Array[String]) { 
  
  }
    
     
  def get_origin_DF(ss: SparkSession, startdate:String, enddate:String):DataFrame = {
    val sc = ss.sparkContext
    
    var All_DF: DataFrame = null
    //until和Range是左闭右开，1是包含的，10是不包含。而to是左右都包含。  for(i <- 0 until 10);  var r = Range(1,10,2);  默认步长1
    
    var start = constUtil.date_to_num_Map(startdate)
    var end = constUtil.date_to_num_Map(enddate)
    
    for(i <- start to end) {
       val filename = "/user/hddtmn/in_common_his_trans/" + constUtil.dateMap(i) + "_correct"
       //println(filename)
       val tmpRdd = sc.textFile(filename).map{str=>
           var tmparr = str.split("\",\"")         
           tmparr = tmparr.map { x => x.toString()}    
           Row.fromSeq(tmparr.toSeq)
       }
   
       var tmp_DF = ss.createDataFrame(tmpRdd, constUtil.schema_251)
    
       val udf_pdate = udf[String, String]{xstr => constUtil.dateMap(i)}
       tmp_DF = tmp_DF.withColumn("pdate", udf_pdate(tmp_DF("pri_key")))   //随便找一个变量进行udf"pri_key"
       //tmp_DF.show(5)
       if(i==start)
         All_DF = tmp_DF
       else
         All_DF = All_DF.unionAll(tmp_DF)
    } 
       
      //All_DF =  All_DF.filter(All_DF("card_attr").=!=("01") )
      All_DF
   }
    
  
    def get_split_DF(ss: SparkSession, startdate:String, enddate:String):DataFrame = {
        var AllData = get_origin_DF(ss, startdate, enddate)
			  AllData.na.fill("isNull")
			
			val udf_substring_BK = udf[String, String]{xstr => 
				if(xstr.length()==8)
				  xstr.substring(0, 4)
				else
				  xstr
			  }
			
			val udf_substring_RG = udf[String, String]{xstr => 
				if(xstr.length()==8)
				  xstr.substring(4, 8)
				else
				  xstr
			  }
			
			AllData = AllData.withColumn("acq_ins_id_cd_BK", udf_substring_BK(AllData("acq_ins_id_cd"))) 
			AllData = AllData.withColumn("fwd_ins_id_cd_BK", udf_substring_BK(AllData("fwd_ins_id_cd")))
			AllData = AllData.withColumn("rcv_ins_id_cd_BK", udf_substring_BK(AllData("rcv_ins_id_cd")))
			AllData = AllData.withColumn("iss_ins_id_cd_BK", udf_substring_BK(AllData("iss_ins_id_cd")))
			AllData = AllData.withColumn("related_ins_id_cd_BK", udf_substring_BK(AllData("related_ins_id_cd")))
			AllData = AllData.withColumn("acpt_ins_id_cd_BK", udf_substring_BK(AllData("acpt_ins_id_cd")))
			AllData = AllData.withColumn("settle_fwd_ins_id_cd_BK", udf_substring_BK(AllData("settle_fwd_ins_id_cd")))
			AllData = AllData.withColumn("settle_rcv_ins_id_cd_BK", udf_substring_BK(AllData("settle_rcv_ins_id_cd")))
			AllData = AllData.withColumn("acct_ins_id_cd_BK", udf_substring_BK(AllData("acct_ins_id_cd")))
			AllData = AllData.withColumn("acq_ins_id_cd_RG", udf_substring_RG(AllData("acq_ins_id_cd"))) 
			AllData = AllData.withColumn("fwd_ins_id_cd_RG", udf_substring_RG(AllData("fwd_ins_id_cd")))
			AllData = AllData.withColumn("rcv_ins_id_cd_RG", udf_substring_RG(AllData("rcv_ins_id_cd")))
			AllData = AllData.withColumn("iss_ins_id_cd_RG", udf_substring_RG(AllData("iss_ins_id_cd")))
			AllData = AllData.withColumn("related_ins_id_cd_RG", udf_substring_RG(AllData("related_ins_id_cd")))
			AllData = AllData.withColumn("acpt_ins_id_cd_RG", udf_substring_RG(AllData("acpt_ins_id_cd")))
			AllData = AllData.withColumn("settle_fwd_ins_id_cd_RG", udf_substring_RG(AllData("settle_fwd_ins_id_cd")))
			AllData = AllData.withColumn("settle_rcv_ins_id_cd_RG", udf_substring_RG(AllData("settle_rcv_ins_id_cd")))
			AllData = AllData.withColumn("acct_ins_id_cd_RG", udf_substring_RG(AllData("acct_ins_id_cd")))
     
			AllData
   }

    
    def get_filled_DF(ss: SparkSession, startdate:String, enddate:String):DataFrame = {
       var AllData = get_split_DF(ss, startdate, enddate)
       val usedArr =  constUtil.usedArr
       
       val udf_replaceEmpty = udf[String, String]{xstr => 
        if(xstr.isEmpty())
          "NANs"
        else
          xstr
       }
       
       for(oldcol <- usedArr){
        val newcol = oldcol + "_filled" 
        AllData = AllData.withColumn(newcol, udf_replaceEmpty(AllData(oldcol)))
       }
     
       AllData
    }


    def get_fraud_join_DF(ss: SparkSession, startdate:String, enddate:String):DataFrame = {
    		val sc = ss.sparkContext
    	  val filename = "xrli/IntelDNN/Fraud_join_2016"
    		 
    	  //sys_tra_no,ar_pri_acct_no,mchnt_cd,trans_dt,fraud_tp
    	  //sys_tra_no, pri_acct_no_conv, mchnt_cd, pdate 

		    val fraud_join_Rdd = sc.textFile(filename).map(str=> str.split("\t")).filter(tmparr=> tmparr(3)>=startdate && tmparr(3)<=enddate).map{ tmparr=>
			    Row.fromSeq(tmparr.toSeq)
    		}
		    
		    val schema_fraud_join = StructType(StructField("sys_tra_no",StringType,true)::StructField("pri_acct_no_conv",StringType,true)::StructField("mchnt_cd",StringType,true)::StructField("pdate",StringType,true)::StructField("fraud_tp",StringType,true)::Nil)
			  val fraud_join_DF = ss.createDataFrame(fraud_join_Rdd, schema_fraud_join) 
			  fraud_join_DF
  
     }
    
    
    def get_filled_DF_byday(ss: SparkSession, date:String):DataFrame = {
       get_filled_DF(ss,date,date)
    }
    
    def get_fraud_join_DF_byday(ss: SparkSession, date:String):DataFrame = {
       get_fraud_join_DF(ss,date,date)
    }
    
    
    def get_processed_DF(ss: SparkSession, input_dir: String):DataFrame = {
       val sc = ss.sparkContext
       
       val Row_RDD = sc.textFile(input_dir).map{str=>
           var tmparr = str.split(",")         
           tmparr = tmparr.map { x => x.toString()}    
           Row.fromSeq(tmparr.toSeq)
       }
       
       val usedArr_filled =  constUtil.usedArr.map{x => x + "_filled"}
       var DF_schema_used = ss.createDataFrame(Row_RDD, funUtil.get_schema(usedArr_filled))
       DF_schema_used
    }
    
       
    def get_labeled_DF(ss: SparkSession, input_dir: String):DataFrame = {
       val sc = ss.sparkContext
       
       val Row_RDD = sc.textFile(input_dir).map{str=>
           var tmparr = str.split(",")
          //.:+ 是添加在尾部，  .+:是添加在头部
           var day_week = funUtil.dayForWeek("2016" + tmparr(1).substring(0,4))
           var hour = tmparr(1).substring(4,6)
           var tmpList = List(tmparr(0).toString()).:+(tmparr(1).toString).:+(day_week.toString).:+(hour.toString).:+(tmparr(2).toString).:+(tmparr(3).toString)
           for(i<- 4 to tmparr.length-1){
             tmpList = tmpList.:+(tmparr(i).toString())
           }
             
           Row.fromSeq(tmpList.toSeq)
       }
       
       println("Row_RDD done")
       var DF_schema_labeled = ss.createDataFrame(Row_RDD, funUtil.get_schema(constUtil.labeledArr))
       //var DF_schema_labeled = ss.createDataFrame(Row_RDD, funUtil.get_schema(constUtil.Labeled_All_Arr))
       DF_schema_labeled
    }     
     
 
   def get_Labeled_All(ss: SparkSession, input_dir: String):DataFrame = {
       val sc = ss.sparkContext
       
       val Row_RDD = sc.textFile(input_dir).map{str=>
           var tmparr = str.split(",")
          //.:+ 是添加在尾部，  .+:是添加在头部
           var tmpList = List(tmparr(0).toString()) 
           for(i<- 1 to tmparr.length-1){
             tmpList = tmpList.:+(tmparr(i).toString())
           }
             
           Row.fromSeq(tmpList.toSeq)
       }
       println("get_Labeled_All: Row_RDD done.")
       var DF_schema_labeled = ss.createDataFrame(Row_RDD, funUtil.get_schema(constUtil.Labeled_All_Arr))
       DF_schema_labeled
    }     
    
    
 
   
   def get_indexed_DF(ss: SparkSession, input_dir: String):DataFrame = {
       val sc = ss.sparkContext
       
       val Row_RDD = sc.textFile(input_dir).map{str=>
           var tmparr = str.split(",")      
           
           var tmpList = List(tmparr(0).toString()).:+(tmparr(1).toDouble)//.:+(tmparr(2).toDouble)//.:+(tmparr(3).toDouble).:+(tmparr(4).toDouble).:+(tmparr(5).toDouble)

           for(i<- 2 to tmparr.length-1){
             tmpList = tmpList.:+(tmparr(i).toDouble)
           }   
           
           
           Row.fromSeq(tmpList.toSeq)
       }
       var DF_schema_used = ss.createDataFrame(Row_RDD, constUtil.schema_labeled)
       DF_schema_used
    }
   
   
   
   def get_FE_DF(ss: SparkSession, input_dir: String):DataFrame = {
       val sc = ss.sparkContext
       
       val Row_RDD = sc.textFile(input_dir).map{str=>
           var tmparr = str.split(",")      
           var tmpList = List(tmparr(0).toString()).:+(tmparr(1).toString)
           for(i<- 2 to tmparr.length-1){
             tmpList = tmpList.:+(tmparr(i).toString)
           }   
           
           
           Row.fromSeq(tmpList.toSeq)
       }
       var DF_schema_used = ss.createDataFrame(Row_RDD, funUtil.get_schema(constUtil.FE_head))
       DF_schema_used
    }    
         
}
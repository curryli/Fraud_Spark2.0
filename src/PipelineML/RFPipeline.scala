package PipelineML

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


//http://www.ibm.com/developerworks/cn/opensource/os-cn-spark-practice5/
object RFPipeline {
 def main(args: Array[String]) {
 //屏蔽日志
 Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
 Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 Logger.getLogger("org").setLevel(Level.OFF);
 Logger.getLogger("akka").setLevel(Level.OFF);
 Logger.getLogger("hive").setLevel(Level.OFF);
 Logger.getLogger("parse").setLevel(Level.OFF);
 
 val conf = new SparkConf().setAppName("Random Forest Classification with ML Pipeline")
 val sc = new SparkContext(conf)
 val sqlCtx = new SQLContext(sc)

 /** Step 1
 * Read the source data file and convert it to be a dataframe with columns named.
 * 3.6216,8.6661,-2.8073,-0.44699,0
 * 4.5459,8.1674,-2.4586,-1.4621,0
 * 3.866,-2.6383,1.9242,0.10645,0
 * 3.4566,9.5228,-4.0112,-3.5944,0
 * 0.32924,-4.4552,4.5718,-0.9888,0
 * ... ...
 */
 val parsedRDD = sc.textFile("xrli/banknote.csv").map(_.split(",")).map(eachRow => {
   val a = eachRow.map(x => x.toDouble)
   (a(0),a(1),a(2),a(3),a(4))
   }
)
 
 val df = sqlCtx.createDataFrame(parsedRDD).toDF("f0","f1","f2","f3","label").cache()

 //下面步骤 2-5是四个 Stage 
 
 /** *
 * Step 2
 * 使用 StringIndexer 去把源数据里的字符 Label，按照 Label 出现的频次对其进行序列编码, 如，0,1,2，…。在本例的数据中，
 * 可能这个步骤的作用不甚明显，因为我们的数据格式良好，Label 本身也只有两种，并且已经是类序列编码的”0”和”1”格式。
 * 但是对于多分类问题或者是 Label 本身是字符串的编码方式，如”High”,”Low”,”Medium”等，那么这个步骤就很有用，转换后的格式，才能被 Spark 更好的处理。
 * */
 val labelIndexer = new StringIndexer()
 .setInputCol("label")
 .setOutputCol("indexedLabel")
 .fit(df)

 /**
 * Step 3
 * 使用 VectorAssembler 从源数据中提取特征指标数据，这是一个比较典型且通用的步骤，因为我们的原始数据集里，经常会包含一些非指标数据，如 ID，Description 等。
 */
 val vectorAssembler = new VectorAssembler()
 .setInputCols(Array("f0","f1","f2","f3"))
 .setOutputCol("featureVector")

 /**
 * Step 4
 * 创建一个随机森林分类器 RandomForestClassifier 实例，并设定相关参数，主要是告诉随机森林算法输入 DataFrame
 *  数据里哪个列是特征向量，哪个是类别标识，并告诉随机森林分类器训练 5 棵独立的子树。
 */
 val rfClassifier = new RandomForestClassifier()
 .setLabelCol("indexedLabel")
 .setFeaturesCol("featureVector")
 .setNumTrees(5)

 /**
 * Step 5
 * 我们使用 IndexToString Transformer 去把之前的序列编码后的 Label 转化成原始的 Label，恢复之前的可读性比较高的 Label，这样不论是存储还是显示模型的测试结果，可读性都会比较高。
 */
 val labelConverter = new IndexToString()
 .setInputCol("prediction")
 .setOutputCol("predictedLabel")
 .setLabels(labelIndexer.labels)

  //步骤 2-5是四个 Stage ，这几个 Stage 都会被用来构建 Pipeline 实例，并且会按照顺序执行，最终我们根据得到的 PipelineModel 实例，进一步调用其 transform 方法，去用训练好的模型预测测试数据集的分类。
 
 
 //Step 6
 //Randomly split the input data by 8:2, while 80% is for training, the rest is for testing.
 val Array(trainingData, testData) = df.randomSplit(Array(0.8, 0.2))

 /**
 * Step 7
 * Create a ML pipeline which is constructed by for 4 PipelineStage objects.
 * and then call fit method to perform defined operations on training data.
 */
 val pipeline = new Pipeline().setStages(Array(labelIndexer,vectorAssembler,rfClassifier,labelConverter))
 
 val model = pipeline.fit(trainingData)

 /**
 *Step 8
 *Perform predictions about testing data. This transform method will return a result DataFrame
 *with new prediction column appended towards previous DataFrame.
 *
 * */
 val predictionResultDF = model.transform(testData)

 /**
 * Step 9
 * Select features,label,and predicted label from the DataFrame to display.
 * We only show 20 rows, it is just for reference.
 */
 predictionResultDF.select("f0","f1","f2","f3","label","predictedLabel").show(20)

 /**
 * Step 10
 * The evaluator code is used to compute the prediction accuracy, this is
 * usually a valuable feature to estimate prediction accuracy the trained model.
 */
 val evaluator = new MulticlassClassificationEvaluator()
 .setLabelCol("label")
 .setPredictionCol("prediction")
 .setMetricName("accuracy")     //选择一哪种方式进行评估      (目前dataframe  支持4种  supports "f1" (default), "weightedPrecision", "weightedRecall", "accuracy")    好像RDD多一点，http://blog.csdn.net/qq_34531825/article/details/52387513?locationNum=4
 
 val predictionAccuracy = evaluator.evaluate(predictionResultDF)
 println("Testing Error = " + (1.0 - predictionAccuracy))
 /**
 * Step 11(Optional)
 * You can choose to print or save the the model structure.
 */
 val randomForestModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
 println("Trained Random Forest Model is:\n" + randomForestModel.toDebugString)
 
 sc.stop()
 }
 
 
}

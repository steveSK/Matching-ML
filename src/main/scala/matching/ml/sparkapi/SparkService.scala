package matching.ml.sparkapi

import java.lang.Exception

import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml._
import org.apache.spark.ml.classification.{LogisticRegression, NaiveBayes, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{Bucketizer, LabeledPoint, QuantileDiscretizer, VectorAssembler}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.classification.{ClassificationModel, NaiveBayesModel, SVMWithSGD}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.functions.from_json

import collection.JavaConverters._


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Service singleton class to wrap spark functionality
  *
  * Created by stefan on 12/19/16.
  */
object SparkService {


  val sparkConfig = new SparkConf()
  sparkConfig.set("spark.default.parallelism","16")
  val sparkSession = SparkSession.builder().config(sparkConfig).getOrCreate()
  sparkSession.sqlContext.setConf("spark.streaming.kafka.consumer.poll.ms","2000");
  sparkSession.sqlContext.setConf("spark.streaming.kafka.maxRatePerPartition","1");
  val streamingContext = new StreamingContext(sparkSession.sparkContext,Seconds(1))
  val JSON_STRUCT_LABEL = "jsontostruct(CAST(value AS STRING))"
  private val LABEL_COLUMN_NAME = "label"
  private val FEATURES_COLUMN_NAME = "features"


  import sparkSession.implicits._


  /**
    * build pipeline model from dataset
    * @param dataset - featured dataset
    * @param stages - pipeline stages to be executed during training
    * @return - pipeline model
    */
  def buildModel(dataset: Dataset[_],stages: Array[_ <: PipelineStage]): PipelineModel = {
    val pipeline = new Pipeline().setStages(stages)
    return pipeline.fit(dataset)
  }


  /**
    * build pipeline model from rdd
    * @param dataset - featured rdd dataset
    * @param stages - pipeline stages to be executed during training
    * @return - pipeline model
    */
  def buildModel(dataset: RDD[LabeledPoint],stages: Array[_ <: PipelineStage]): PipelineModel = {
    val pipeline = new Pipeline().setStages(stages)
    return pipeline.fit(dataset.toDF(LABEL_COLUMN_NAME,FEATURES_COLUMN_NAME))
  }

  /**
    * load pipeline model from local storage
    * @param pathToModel local path
    * @return pipeline model
    */
  def loadModel(pathToModel: String): PipelineModel = {
    try {
      PipelineModel.load(pathToModel)
    } catch {
      case e: Exception => null
    }
  }

  /***
    * build linear support vector machines algorithm with gradient descent to train ML model
    *
    * @param numIteration Number of iterations of gradient descent to run.
    * @param stepSize  Step size to be used for each iteration of gradient descent.
    * @param regparam  Regularization parameter.
    * @param minBatchFraction Fraction of data to be used per iteration.
    * @return SVM algorithm
    */
  def buildSVMachines(numIteration: Int,stepSize: Double, regparam: Double,minBatchFraction: Double): SVMWithSGD = {
    val svmAlg = new SVMWithSGD()
    svmAlg.optimizer
      .setNumIterations(numIteration)
      .setRegParam(regparam)
      .setStepSize(stepSize)
      .setMiniBatchFraction(minBatchFraction)
      .setUpdater(new L1Updater)
    return svmAlg
  }


  /***
    *  build Naive Bayes algorithm to train ML model
    * @return Naive Bayes alg
    */
  def buildNaiveBayes() : NaiveBayes = {
    val modelType = "multinomial"
    return new NaiveBayes().setModelType(modelType);
  }

  /***
    * build Random Forest algorithm to train ML model
    * @param numClasses number of classes for classification
    * @param numTrees  number of trees in the forest.
    * @param maxBins  number of bins used when discretizing continuous features.
    * @param maxDepth maximum depth of each tree in the forest
    * @param featureSubsetStrategy number of features to use as candidates for splitting at each tree node. The number is specified as a fraction or function of the total number of features
    * @param impurity the node impurity is a measure of the homogeneity of the labels at the node.
    * @param maxMemory amount of memory to be used for collecting sufficient statistics.
    * @return
    */
  def buildRandomForest(numClasses: Int, numTrees: Int, maxBins: Int, maxDepth: Int, featureSubsetStrategy: String, impurity: String, maxMemory: Int): RandomForestClassifier = {
    val categoricalFeaturesInfo = Map[Int, Int]()
    return new RandomForestClassifier()
      .setNumTrees(numTrees)
      .setMaxBins(maxBins)
      .setMaxDepth(maxDepth)
      .setImpurity(impurity)
      .setMaxMemoryInMB(maxMemory)
      .setLabelCol(LABEL_COLUMN_NAME)
      .setFeaturesCol(FEATURES_COLUMN_NAME)
      .setFeatureSubsetStrategy(featureSubsetStrategy)
  }

  /***
    * build Logistic Regression algorithm to train ML model
    * @param maxIter Number of iterations of gradient descent to run
    * @param regParam - regulasition parameter
    * @param elasticNetParam
    * @param family param for the name of family which is a description of the label distribution
    *               to be used in the mode
    * @param treshold set threshold in binary classification, in range [0, 1].
    *                  if the estimated probability of class label 1 is greater than threshold, then predict 1,
    *                  else 0. A high threshold encourages the model to predict 0 more often;
    *                  a low threshold encourages the model to predict 1 more often.
    * @return
    */
  def buildLogisticRegression(maxIter: Int, regParam: Double, elasticNetParam: Double,family: String,treshold: Double) : LogisticRegression = {

    return new LogisticRegression()
      .setMaxIter(maxIter)
      .setRegParam(regParam)
      .setElasticNetParam(elasticNetParam)
      .setFamily(family)
      .setThreshold(treshold)
  }

  /***
    * create kafka stream receiving messages from Kafka broker
    *
    * @param topic topic to read
    * @param kafkaParams kafka consumer configuration
    * @return
    */
  def createKafkaStream(topic: String, kafkaParams : Map[String,Object]) = {
    KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      Subscribe[String, String](Array(topic), kafkaParams))
     sparkSession
  }

  /***
    * Create spark structured streaming dataset using kafka with schema
    * @param topic topic to read
    * @param kafkaParams kafka consumer configuration
    * @param schema schema to define dataset columns ant types
    * @return structured streaming dataset
    */
  def createKafkaStreamDataSet(topic: String, kafkaParams : Map[String,Object],schema: StructType): Dataset[Row] ={
    val rawKafkaDF = sparkSession.sqlContext.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",kafkaParams.get("bootstrap.servers").get.toString)
      .option("subscribe",topic)
      .load()

    rawKafkaDF.select(from_json($"value".cast(StringType),schema)).select(flattenSchema(schema,JSON_STRUCT_LABEL) : _*)
  }

  /***
    * Create spark structured streaming dataset using kafka without schema all columns are converted to String
    * @param topic topic to read
    * @param kafkaParams kafka consumer configuration
    * @return
    */
  def createKafkaStreamStringDataSet(topic: String, kafkaParams : Map[String,Object]): Dataset[String] = {
    val rawKafkaDF = sparkSession.sqlContext.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaParams.get("bootstrap.servers").get.toString)
      .option("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
      .option("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
      .option("subscribe", topic)
      .load()

    rawKafkaDF.select($"value".cast(StringType)).as(Encoders.STRING)
  }

  /***
    * flatten schema of nested types
    * @param schema schema
    * @param prefix prefix which flatten
    * @return flatten schema as array of columns
    */
  def flattenSchema(schema: StructType, prefix: String = null) : Array[Column] = {
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)
      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _ => Array(new Column(colName))
      }
    })
  }

  /***
    * Evaluate classification model with test data, using BinaryClassificationEvaluator, returning area under the ROC
    * @param testDF test dataset
    * @param model model to test
    * @return area under the ROC curve
    */
  def evaluateTestDataML(testDF: Dataset[_], model :  Transformer): Double ={
    val predictions = model.transform(testDF.toDF())
    predictions.select("prediction", LABEL_COLUMN_NAME, FEATURES_COLUMN_NAME).show(5)
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol(LABEL_COLUMN_NAME)
      .setRawPredictionCol("prediction")
      .setMetricName("areaUnderROC")
    return evaluator.evaluate(predictions)
  }

  /***
    *  Evaluate classification model from spark.mllib with test data, using MulticlassMetrics
    * @param testRDD test dataset rdd
    * @param model  model to test
    * @return accuracy
    */
  def evaluateTestDataMLlib(testRDD: RDD[org.apache.spark.mllib.regression.LabeledPoint], model :   ClassificationModel): Double ={
    val scoreAndLabels = testRDD.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }
    val metrics = new org.apache.spark.mllib.evaluation.MulticlassMetrics(scoreAndLabels)
    metrics.accuracy
  }

  /***
    * use cross validation setup, to train the model, spark pick the best model available after cross-validation
    * @param trainDF train dataset to build crossvalidation setuo
    * @param paramGrid grid of params
    * @param pipilineArray pipeline array of different training stages
    * @param nFolds - number of folds to cross-validate
    * @return
    */
  def crossValidate(trainDF: DataFrame, paramGrid: Array[ParamMap], pipilineArray: Array[_ <: PipelineStage], nFolds : Int) : Transformer = {
    val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol(LABEL_COLUMN_NAME)
    .setPredictionCol("prediction")
    .setMetricName("accuracy")
    val pipeline = new Pipeline().setStages(pipilineArray)
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(nFolds)

    return cv.fit(trainDF)
  }

  /***
    * dividing dataset on train and test dataset returned as RDD with labeledPoints
    * @param rawDF - raw dataset
    * @param weights weights on division of dataset
    * @param seed seed for division
    * @param ignored - ignored columns in dataset
    * @return tupple with RDD labeled points dataset
    */
  def getTrainAndTestData(rawDF : Dataset[Row], weights: Array[Double], seed: Int, ignored: List[String]): (RDD[LabeledPoint],
    RDD[LabeledPoint]) ={
    val listDFs =  rawDF.randomSplit(weights,seed).map(x => x.toDF())
    val trainDF = listDFs(0)
    val testDF = listDFs(1)

    return (transformRawDFToLabeledPointRDDML(trainDF,ignored), transformRawDFToLabeledPointRDDML(testDF,ignored))
  }


  /***
    * dividing dataset on train and test dataset returned as Dataset[Row]
    * @param rawDF - raw dataset
    * @param weights weights on division of dataset
    * @param labelName label name
    * @param seed seed for division
    * @param ignored - ignored columns in dataset
    * @return touple of
    */
  def getTrainAndTestDataDF(rawDF : Dataset[Row], weights: Array[Double],labelName: String, seed: Int, ignored: List[String]): (Dataset[Row],
    Dataset[Row]) ={
    val listDFs =  rawDF.randomSplit(weights,seed).map(x => x.toDF())
    val trainDF = listDFs(0)
    val testDF = listDFs(1)

    return (transformRawDFToVectorDF(trainDF,ignored,labelName,true), transformRawDFToVectorDF(testDF,ignored,labelName,true))
  }

  /***
    * dividing dataset on train and test dataset returned as RDD with labeledPoints as Mllib
    * @param rawDF - raw dataset
    * @param weights weights on division of dataset
    * @param seed seed for division
    * @param ignored - ignored columns in dataset
    * @return tupple with RDD labeled points dataset
    */
  def getTrainAndTestDataMLlib(rawDF : Dataset[Row], weights: Array[Double], seed: Int, ignored: List[String]): (RDD[org.apache.spark.mllib.regression.LabeledPoint],
    RDD[org.apache.spark.mllib.regression.LabeledPoint]) ={
    val listDFs =  rawDF.randomSplit(weights,seed).map(x => x.toDF())
    val trainDF = listDFs(0)
    val testDF = listDFs(1)

    return (transformRawDFToLabeledPointRDDMLlib(trainDF,ignored), transformRawDFToLabeledPointRDDMLlib(testDF,ignored))
  }

  /***
    * Load data from CSV file to DataFrame dataset
    *
    * @param filepath - csv file path
    * @param delimeter - delimeter to broke down the lines in file to columns
    * @param header - if header is included or not
    * @return return Dataframe dataset
    */
  def loadDataFromCSV(filepath: String,delimeter: String,header: Boolean): Dataset[Row] = {
    sparkSession.read.option("header", header.toString)
      .option("delimiter", delimeter)
      .option("inferSchema", "true")
      .format("csv").load(filepath)
  }

  /***
    * Load data from Kafka to dataset for batch processing, is necessary to define start and end offsets
    *
    * @param topic name of topic to read
    * @param offsets start and end offset to read
    * @param kafkaParams kafkaParams to connect to Kafka
    * @return dataset loaded from Kafka
    */
  def loadDataFromKafka(topic: String,offsets: Array[(Int,Int)], kafkaParams : Map[String,Object]): Dataset[Row] = {
      val offsetRange = offsets.map(x => OffsetRange(topic,x._1,0,x._2))
      val rdd = KafkaUtils.createRDD[String,String](sparkSession.sparkContext,kafkaParams.asJava,offsetRange,LocationStrategies.PreferConsistent).map(x => x.value())
      sparkSession.read.json(rdd)
  }

  /***
    * transform row dataset to labeledpoint rdd which represents dataset with label and features, ready to be used for training
    *
    * @param df dataset to transform
    * @param ignored columns to ignore
    * @return labeledpoint rdd
    */
  def transformRawDFToLabeledPointRDDML(df: Dataset[Row], ignored: List[String]): RDD[LabeledPoint] = {
     val featInd = df.columns.diff(ignored).map(df.columns.indexOf(_))
     val labelIndex = df.columns.indexOf(LABEL_COLUMN_NAME)
     df.rdd.map(x => LabeledPoint(x.getString(labelIndex).toDouble, org.apache.spark.ml.linalg.Vectors.dense(featInd.map(x.get(_).toString.toDouble))))
  }

  /***
    * transform row dataset to labeledpoint rdd which represents dataset with label and features, ready to be used for training (Mllib version)
    *
    * @param df dataset to transform
    * @param ignored columns to ignore
    * @return labeledpoint rdd
    */
  def transformRawDFToLabeledPointRDDMLlib(df: Dataset[Row],ignored: List[String]): RDD[org.apache.spark.mllib.regression.LabeledPoint] = {
    val featInd = df.columns.diff(ignored).map(df.columns.indexOf(_))
    val labelIndex = df.columns.indexOf(LABEL_COLUMN_NAME)
    df.rdd.map(x => org.apache.spark.mllib.regression.LabeledPoint(x.getDouble(labelIndex),
      org.apache.spark.mllib.linalg.Vectors.dense(featInd.map(x.getDouble(_)))))

  }

  /***
    *  takes a column with continuous features and outputs a column with binned categorical features using spark QuantileDiscretizer
    *
    * @param df input dataset
    * @param inputColumn input columnt
    * @param outputColumn output column
    * @param bucketsNum number of buckets
    * @return dataset with discretized column
    */
  def discretizeColumn(df: Dataset[Row],inputColumn: String, outputColumn: String, bucketsNum: Int): Dataset[Row] ={
    val discretizer = new QuantileDiscretizer()
      .setInputCol(inputColumn)
      .setOutputCol(outputColumn)
      .setNumBuckets(bucketsNum)

    discretizer.fit(df).transform(df)
  }

  /****
    * transforms a column of continuous features to a column of feature buckets, where the buckets are specified by users.
    *
    * @param df input dataset
    * @param splits number of bucket splits
    * @param inputColumn input column
    * @param outputColumn output column
    * @return dataset with bucketized column
    */
  def bucketizeColumn(df: Dataset[Row],splits: Array[Double], inputColumn: String, outputColumn: String): Dataset[Row] = {
    val bucketizer = new Bucketizer()
      .setInputCol(inputColumn)
      .setOutputCol(outputColumn)
      .setHandleInvalid("skip")
      .setSplits(splits)

    bucketizer.transform(df)
  }

  /***
    * transform raw dataset to vector feature dataset with label and feature columns
    * @param df - raw dataset
    * @param ignored - columns to be ignored (not included in features)
    * @param labelColumn - label column
    * @param removeIgnored whether ignored columns should be removed or not
    * @return return dataset
    */
  def transformRawDFToVectorDF(df: Dataset[Row], ignored: List[String],labelColumn: String, removeIgnored : Boolean): Dataset[Row] = {
    var dfToDouble = df
    import org.apache.spark.sql.functions.udf
    val toDouble:String => Double = ( _.toDouble)
    val toDoubleUDF = udf(toDouble)
    df.columns.diff(LABEL_COLUMN_NAME :: ignored).foreach(
      x => dfToDouble = dfToDouble.select($"*",toDoubleUDF(dfToDouble(x))).drop(x).withColumnRenamed("UDF(" + x + ")",x))
    if(df.columns.contains(labelColumn)) {
      dfToDouble = dfToDouble.withColumn(labelColumn, toDoubleUDF(dfToDouble(labelColumn)))
    }

    dfToDouble.printSchema()
    val featureColumns = df.columns.diff(labelColumn :: ignored)
   new VectorAssembler()
      .setInputCols(featureColumns)
      .setOutputCol(FEATURES_COLUMN_NAME).transform(dfToDouble).drop(df.columns.diff(labelColumn :: ignored):_*)
  }


  /***
    * singleton sparkSession
    * @return sparkSession
    */
  def getSparkSession(): SparkSession = {
    sparkSession
  }


}

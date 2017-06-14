package matching.ml.sparkapi

import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.feature.{LabeledPoint, VectorSlicer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * A class to evaluate machine learning algorithms
  *
  * Created by stefan on 2/14/17.
  */
class AlgorithmEvaluator {

  val service = SparkService

  import service.sparkSession.implicits._

  def evaluateAlgorithmSetup(datasets: (Dataset[Row],Dataset[Row]),stages: Array[_ <: PipelineStage], modelDestination: Option[String] = None): Double = {
    val model = service.buildModel(datasets._1, stages)
    if(modelDestination.isDefined){
      model.save(modelDestination.get)
    }
    service.evaluateTestDataML(datasets._2,model)
  }

  /***
    *  evaluate influence of one feature, by removing other features from training dataset
    *
    * @param datasets tupple containing training and test dataset
    * @param stages pipeline stages for learning
    * @param index index of feature to test
    * @return return given metric(area underroc etc) of given model
    */
  def evaluateFeature(datasets: (Dataset[Row],Dataset[Row]),stages: Array[_ <: PipelineStage], index: Int) : Double = {
    val trainDF = datasets._1.toDF("label","ufeatures");
    val testDF = datasets._2.toDF("label","ufeatures");
    val transTrainDF = sliceFeatures(trainDF,"ufeatures","features",Array(index))
    val transTestDF = sliceFeatures(testDF,"ufeatures","features",Array(index))
    val model = service.buildModel(transTrainDF,stages)
    service.evaluateTestDataML(transTestDF,model)
  }


  /***
    *  evaluate influence of feature agaisnt all features, by removing particular feature from training dataset
    *
    * @param datasets tupple containing training and test dataset
    * @param stages pipeline stages for learning
    * @param index index of feature to tremove
    * @numOfFeatures number of features in dataset
    * @return return given metric(area underroc etc) of given model
    */
  def evaluateAgainstFeature(datasets: (Dataset[Row],Dataset[Row]),stages: Array[_ <: PipelineStage], index: Int, numOfFeatures: Int) : Double = {
    val trainDF = datasets._1.toDF("label","ufeatures");
    val testDF = datasets._2.toDF("label","ufeatures");
    val range = 0 to (numOfFeatures - 1)

    val indexes = range.toArray.filter(_ != index)
    val transTrainDF = sliceFeatures(trainDF,"ufeatures","features",indexes)
    val transTestDF = sliceFeatures(testDF,"ufeatures","features",indexes)
    val model = service.buildModel(transTrainDF,stages)
    service.evaluateTestDataML(transTestDF,model)
  }

  /***
    * slice subset of features from dataset
    * @param dataset dataset to slice
    * @param inputCol input feature column
    * @param outputCol output feature column
    * @param featuresToPick
    * @return
    */
  private def sliceFeatures(dataset : Dataset[_], inputCol : String, outputCol: String, featuresToPick: Array[Int]): Dataset[_] = {
    val slicer = new VectorSlicer().setInputCol(inputCol).setOutputCol(outputCol)
    slicer.setIndices(featuresToPick)
    slicer.transform(dataset).drop(inputCol)
  }

}

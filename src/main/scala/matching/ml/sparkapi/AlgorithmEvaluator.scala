package matching.ml.sparkapi

import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature.{LabeledPoint, VectorSlicer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by stefan on 2/14/17.
  */
class AlgorithmEvaluator(service: SparkService) {

  import service.sparkSession.implicits._

  def evaluateAlgorithmSetup(datasets: (RDD[LabeledPoint],RDD[LabeledPoint]),stages: Array[_ <: PipelineStage]): Double = {
    val model = service.buildModel(datasets._1, stages)
    service.evaluateTestDataML(datasets._2.toDF("label","features"),model)
  }

  def evaluateFeature(datasets: (RDD[LabeledPoint],RDD[LabeledPoint]),stages: Array[_ <: PipelineStage], index: Int) : Double = {
    val trainDF = datasets._1.toDF("label","ufeatures");
    val testDF = datasets._2.toDF("label","ufeatures");
    val transTrainDF = sliceFeatures(trainDF,"ufeatures","features",Array(index))
    val transTestDF = sliceFeatures(testDF,"ufeatures","features",Array(index))
    val model = service.buildModel(transTrainDF,stages)
    service.evaluateTestDataML(transTestDF,model)
  }

  def evaluateAgainstFeature(datasets: (RDD[LabeledPoint],RDD[LabeledPoint]),stages: Array[_ <: PipelineStage], index: Int, numOfFeaures: Int) : Double = {
    val trainDF = datasets._1.toDF("label","ufeatures");
    val testDF = datasets._2.toDF("label","ufeatures");
    val range = 0 to (numOfFeaures - 1)

    val indexes = range.toArray.filter(_ != index)
    val transTrainDF = sliceFeatures(trainDF,"ufeatures","features",indexes)
    val transTestDF = sliceFeatures(testDF,"ufeatures","features",indexes)
    val model = service.buildModel(transTrainDF,stages)
    service.evaluateTestDataML(transTestDF,model)
  }

  private def sliceFeatures(dataset : Dataset[_], inputCol : String, outputCol: String, featuresToPick: Array[Int]): Dataset[_] = {
    val slicer = new VectorSlicer().setInputCol(inputCol).setOutputCol(outputCol)
    slicer.setIndices(featuresToPick)
    slicer.transform(dataset).drop(inputCol)
  }

}

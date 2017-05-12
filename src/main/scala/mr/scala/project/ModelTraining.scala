package mr.scala.project

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.RandomForest

/**
 * This class trains the random forest classification model.
 */
object ModelTraining {

  /**
   * This driver method runs the job to train the random forest classification model 
   * and writes the trained model as output.
   */
  def main(args: Array[String]): Unit =
    {
      // Initialize local variables
      var inputPath = "input"
      var outputPath = "output"

      println("ModelTraining Program Started.");
      val t1 = System.nanoTime()

      // Update local variables from args array
      if (args.length == 2) {
        println("Input Arguments :: " + args(0) + " " + args(1))
        inputPath = args(0)
        outputPath = args(1)
      }

      // create spark context
      val conf = new SparkConf().setAppName("ModelTraining")
      val sc = new SparkContext(conf)

      // read and convert labeled data set to LabelePointRDD
      val inputLabeledData = sc.textFile(inputPath + "/labeled")
      val labeledData = inputLabeledData.flatMap(HelperFunctions.getLabeledPoint)

      // Train a RandomForest model.
      val numClasses = 2
      val categoricalFeaturesInfo = Map[Int, Int]((2,12), (3,5), (8,2), (9,2), (10,2), (11,2), (12,2))
      val numTrees = 33
      val featureSubsetStrategy = "auto"
      val impurity = "gini"
      val maxDepth = 20
      val maxBins = 32

      val model = RandomForest.trainClassifier(labeledData, numClasses, categoricalFeaturesInfo,
        numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

      // Save model
      model.save(sc, outputPath + "TModel")

      val t2 = System.nanoTime()
      println("ModelTraining Program finished successfully.")
      println("Total Running Time(s) = " + (t2 - t1) / 1000000000)
      sc.stop()
    }
}

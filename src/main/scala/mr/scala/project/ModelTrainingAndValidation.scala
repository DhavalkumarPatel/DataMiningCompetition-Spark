package mr.scala.project

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.RandomForest

/**
 * This class trains the random forest classification model on 70%
 * labeled data set and validate/measures the accuracy on the remaining
 * 30% labeled data.
 */
object ModelTrainingAndValidation {

  def main(args: Array[String]): Unit =
    {
      // Initialize local variables
      var inputPath = "input"
      var outputPath = "output"

      println("Model Training and Validation Program started.");
      val t1 = System.nanoTime()

      // Update local variables from args array
      if (args.length == 2) {
        println("Input Arguments :: " + args(0) + " " + args(1))
        inputPath = args(0)
        outputPath = args(1)
      }

      // create spark context
      val conf = new SparkConf().setAppName("ModelTrainingAndValidationProgram")
      val sc = new SparkContext(conf)

      val inputLabeledData = sc.textFile(inputPath + "/labeled")
      val labeledData = inputLabeledData.flatMap(HelperFunctions.getLabeledPoint)

      // Split the data into training and test sets (30% held out for testing)
      val splits = labeledData.randomSplit(Array(0.7, 0.3))
      val (trainingData, testData) = (splits(0), splits(1))

      // Train a RandomForest model.
      val numClasses = 2
      val categoricalFeaturesInfo = Map[Int, Int]((2,12), (3,5), (8,2), (9,2), (10,2), (11,2), (12,2))
      val numTrees = 33
      val featureSubsetStrategy = "auto"
      val impurity = "gini"
      val maxDepth = 20
      val maxBins = 32

      val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
        numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

      // Evaluate model on test instances and compute test error
      val labelAndPreds = testData.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }
      
      // Measure the test error
      val totalPreds = testData.count()
      val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble
      val accuracy = (totalPreds - testErr) / totalPreds
      
      // Save model
      model.save(sc, outputPath + "TVModel")

      val t2 = System.nanoTime()
      println("Model Training and Validation Program finished successfully.")
      println("Total Running Time(s) = " + (t2 - t1) / 1000000000)
      println("Total Predicted Records = " + totalPreds)
      println("Total Records Predicted Wrong = " + testErr)
      println("Accuracy = " + accuracy)
      sc.stop()
    }
}

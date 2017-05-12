package mr.scala.project

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.model.RandomForestModel

/**
 * This class predicts the bird presence for unlabeled data set. 
 */
object ModelPrediction 
{
  /**
   * This driver method runs the job to predict the bird presence for unlabeled data set
   * using trained model.
   */
  def main(args: Array[String]): Unit =
    {
      // Initialize local variables
      var inputPath = "input"
      var outputPath = "output"

      println("ModelPrediction Program Started.");
      val t1 = System.nanoTime()

      // Update local variables from args array
      if (args.length == 2) {
        println("Input Arguments :: " + args(0) + " " + args(1))
        inputPath = args(0)
        outputPath = args(1)
      }

      // create spark context
      val conf = new SparkConf().setAppName("ModelPrediction")
      val sc = new SparkContext(conf)

      // load unlabeled data set
      val inputUnlabeledData = sc.textFile(inputPath + "/unlabeled")
      
      // convert unlabeled data to LabelPoint RDD
      val unLabeledData = inputUnlabeledData.flatMap(HelperFunctions.getUnlabeledPoint)
      
       // load the model
      val model = RandomForestModel.load(sc, outputPath + "TModel")
      
      // prediction
      val labelAndPreds = unLabeledData.map { point =>
        val prediction = model.predict(point.features)
        "S" + point.label.toLong + ", " + prediction.toInt
      }
      
      // generate header of final file
      val header = sc.parallelize(Array("SAMPLING_EVENT_ID, SAW_AGELAIUS_PHOENICEUS"))
      
      // write output in one file
      sc.parallelize(header.union(labelAndPreds).collect(), 1).saveAsTextFile(outputPath + "/PredictionOutput")
      
      val t2 = System.nanoTime()
      println("ModelPrediction Program finished successfully.")
      println("Total Running Time(s) = " + (t2 - t1) / 1000000000)
      sc.stop()
    }
}
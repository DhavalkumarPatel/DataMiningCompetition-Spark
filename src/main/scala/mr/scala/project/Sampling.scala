package mr.scala.project

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * This class generates the small subset of the main labeled data 
 * set from full labeled data set to work on local machine.
 */
object Sampling {

  def main(args: Array[String]): Unit =
    {
      // Initialize local variables
      var inputPath = "input"
      var outputPath = "output"

      println("Sampling Program started.");
      val t1 = System.nanoTime()

      // Update local variables from args array
      if (args.length == 2) {
        println("Input Arguments :: " + args(0) + " " + args(1))
        inputPath = args(0)
        outputPath = args(1)
      }

      // create spark context
      val conf = new SparkConf().setAppName("Sampling")
      val sc = new SparkContext(conf)

      // take a sample of data and write it to output folder
      sc.textFile(inputPath + "/labeled").sample(false, 0.1, 968574236524178L).saveAsTextFile(outputPath)
      
      val t2 = System.nanoTime()
      
      println("Sampling Program successfully finished.");
      println("Total Running Time(s) = " + (t2 - t1) / 1000000000)
      sc.stop()
    }
}
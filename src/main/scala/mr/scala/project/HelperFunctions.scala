package mr.scala.project

import scala.collection.mutable.ListBuffer

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * This class provides helper functions to other driver classes.
 */
object HelperFunctions {

  /**
   * This method parses the given line of labeled data set and generates the 
   * LabeledPoint for it.
   */
  def getLabeledPoint(line: String): Array[LabeledPoint] =
    {
      val lpBuf = new ListBuffer[LabeledPoint]

      if (null != line && !line.contains("SAMPLING_EVENT_ID")) 
      {
        lpBuf += parseLineToLabeledPoint(line, true)
      }

      return lpBuf.toArray
    }

  /**
   * This method parses the given line of unlabeled data set and generates the 
   * LabeledPoint for it. SampleId is stored as a label double value. 
   */
  def getUnlabeledPoint(line: String): Array[LabeledPoint] =
    {
      val lpBuf = new ListBuffer[LabeledPoint]

      if (null != line && !line.contains("SAMPLING_EVENT_ID")) 
      {
        lpBuf += parseLineToLabeledPoint(line, false)
      }

      return lpBuf.toArray
    }

  /**
   * This method parses the line of labeled or unlabeled data set to LabeledPoint.
   */
  def parseLineToLabeledPoint(line: String, isLabeledData: Boolean): LabeledPoint =
    {
      // features considered
      val features = Array(2, 3, 5, 11, 12, 13, 14, 16, 24, 25, 45, 53, 139, 955, 956, 957, 958, 959, 960, 962, 963, 964, 965, 966, 967, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 1015, 1078, 1079, 1080, 1081, 1082, 1083)
      val columns = line.split(",")

      var label = 0.0

      // store actual label for labeled data set and sample id in case of unlabeled data set
      if (isLabeledData) {
        if (columns(26).equals("X") || columns(26).toDouble > 0.0) {
          label = 1.0
        }
      } else {
        label = columns(0).substring(1).toDouble
      }

      // indices and values for sparse vector generation
      val indices = new ListBuffer[Int]
      val values = new ListBuffer[Double]

      for (index <- 0 until features.length) {
        val colIndex = features(index)
        
        if (null != columns(colIndex) && !"".equals(columns(colIndex)) && !"?".equals(columns(colIndex)) && (!"X".equalsIgnoreCase(columns(colIndex)) || colIndex == 24 || colIndex == 25 || colIndex == 45 || colIndex == 53 || colIndex == 139)) {
          indices += index

          // convert month to categorical features
          if (colIndex == 5) 
          {
            values += (columns(colIndex).toDouble - 1.0)
          } 
          // convert count type to categorical features
          else if (colIndex == 11) 
          {
            if (columns(colIndex).equalsIgnoreCase("P21")) {
              values += 0.0
            } else if (columns(colIndex).equalsIgnoreCase("P22")) {
              values += 1.0
            } else if (columns(colIndex).equalsIgnoreCase("P23")) {
              values += 2.0
            } else if (columns(colIndex).equalsIgnoreCase("P34")) {
              values += 3.0
            } else {
              values += 4.0
            }
          } 
          // convert other similar species to categorical features
          else if (colIndex == 24 || colIndex == 25 || colIndex == 45 || colIndex == 53 || colIndex == 139) 
          {
            var colVal = 0.0

            if (columns(colIndex).equals("X") || columns(colIndex).toDouble > 0.0) {
              colVal = 1.0
            }

            values += colVal
          } 
          // other continuous features
          else 
          {
            values += columns(colIndex).toDouble
          }
        }
      }

      return LabeledPoint(label, Vectors.sparse(features.length, indices.toArray, values.toArray))
    }
}
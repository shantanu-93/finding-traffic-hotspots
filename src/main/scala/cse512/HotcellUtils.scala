package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def calculateSurroundingCells(X: Int, Y: Int, Z: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int): Int = {
    var neighborX = 3; var neighborY = 3; var neighborZ = 3;

    if (X == minX || X == maxX) {
      neighborX -= 1
    }
    if (Y == minY || Y == maxY) {
      neighborY -= 1
    }
    if (Z == minZ || Z == maxZ) {
      neighborZ -= 1
    }
    return neighborX * neighborY * neighborZ - 1;
  }


  def calZScore(surroundingCells: Int, totalCount: Int, numCells: Int, x: Int, y: Int, z: Int, mean: Double,
                      standardDeviation: Double): Double =
  {
    val meanSurCellsWeights = (mean * surroundingCells.toDouble)
    val summationSuroundingCells = (totalCount.toDouble)

    val neighorContribtution = summationSuroundingCells - meanSurCellsWeights
    val contributionOfall = standardDeviation * math.sqrt((((numCells.toDouble * surroundingCells.toDouble)) -
      (surroundingCells.toDouble * surroundingCells.toDouble)) / (numCells.toDouble - 1))
    return (neighorContribtution / contributionOfall.toDouble)
  }
}

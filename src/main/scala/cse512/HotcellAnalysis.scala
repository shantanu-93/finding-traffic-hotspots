package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  pickupInfo.createOrReplaceTempView("pickupInfoData")

  pickupInfo = spark.sql("select x as X,y as Y,z as Z from pickupInfoData where x>= " + minX + " and x<= " + maxX + " " +
    "and y>= " + minY + " and y<= " + maxY + " and  z>= " + minZ + " and z<= " + maxZ + " order by z, y, x");
  pickupInfo.createOrReplaceTempView("CellsInBoundaryData")

  pickupInfo = spark.sql("select X, Y, Z, count(*) AS Count from CellsInBoundaryData group by X, Y, Z ORDER BY Z, Y, X")
  pickupInfo.createOrReplaceTempView("CellAndCountData")

  val totalCountOfAllCell = spark.sql("select  sum(Count) as totalCount from CellAndCountData")
  totalCountOfAllCell.createOrReplaceTempView("totalCountOfAllCellData")

  val mean = (totalCountOfAllCell.first().getLong(0).toDouble / numCells.toDouble)
  print("the mean is ")
  println(mean)

  spark.udf.register("squaring", (inputX: Int) => (((inputX * inputX).toDouble)))

  val squaredCount = spark.sql("select sum(squaring(Count)) as sequaredSum from CellAndCountData")
  squaredCount.createOrReplaceTempView("squaredCountData")

  val standardDeviation = scala.math.sqrt(((squaredCount.first().getDouble(0) / numCells.toDouble) -
    (mean.toDouble * mean.toDouble)))
  print("the standard deviation is ")
  println(standardDeviation)

  spark.udf.register("SurroundingCellsCount", (X: Int, Y: Int, Z: Int, minX: Int, maxX: Int, minY: Int, maxY: Int,
                                          minZ: Int, maxZ: Int) =>
    ((HotcellUtils.calculateSurroundingCells(X, Y, Z, minX, minY, minZ, maxX, maxY, maxZ))))


  val surroundingCells = spark.sql("select SurroundingCellsCount(CCD1.X, CCD1.Y, CCD1.Z, " + minX + "," +
    maxX + "," + minY + "," + maxY + "," + minZ + "," + maxZ + ") "
    + "as surr_count, CCD1.X as X, CCD1.Y as Y, CCD1.Z as Z, sum(CCD2.Count) as surr_fre " +
    "from CellAndCountData as CCD1, CellAndCountData as CCD2 "
    + "where (CCD2.X = CCD1.X+1 or CCD2.X = CCD1.X or CCD2.X = CCD1.X-1) " +
    "and (CCD2.Y = CCD1.Y+1 or CCD2.Y = CCD1.Y or CCD2.Y = CCD1.Y-1) "
    + "AND (CCD2.Z = CCD1.Z+1 or CCD2.Z = CCD1.Z or CCD2.Z = CCD1.Z-1) " +
    "GROUP BY CCD1.Z, CCD1.Y, CCD1.X ORDER BY CCD1.Z, CCD1.Y, CCD1.X")
  surroundingCells.createOrReplaceTempView("surroundingCellsData")

  spark.udf.register("ZScore", (surroundingCells: Int, totalCount: Int, numCells: Int, x: Int, y: Int,
                                z: Int, mean: Double, standardDeviation: Double) =>
    ((HotcellUtils.calZScore(surroundingCells, totalCount, numCells, x, y, z, mean, standardDeviation))))

  pickupInfo = spark.sql("select ZScore(surr_count, surr_fre, " + numCells + ", x, y, z,"
    + mean + ", " + standardDeviation + ") as Gi_Or, X, Y, Z from surroundingCellsData order by Gi_Or desc");
  pickupInfo.createOrReplaceTempView("zScoreData")
  pickupInfo.show()

  pickupInfo = spark.sql("select X, Y, Z from zScoreData")
  pickupInfo.createOrReplaceTempView("results")
  pickupInfo.show()

  return pickupInfo // YOU NEED TO CHANGE THIS PART
}
}

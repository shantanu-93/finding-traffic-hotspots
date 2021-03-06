package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    val Rectangle: Array[Double] = queryRectangle.split(',').map(x => x.trim.toDouble)
    val Point: Array[Double] = pointString.split(',').map(x => x.trim.toDouble)
    val x = Point(0)
    val y = Point(1)


    var ymin: Double = 0.0
    var ymax: Double = 0.0

    if (Rectangle(3) > Rectangle(1)) {
      ymin = Rectangle(1)
      ymax = Rectangle(3)
    }
    else if (Rectangle(3) < Rectangle(1)) {
      ymin = Rectangle(3)
      ymax = Rectangle(1)
    }


    var xmin: Double = 0.0
    var xmax: Double = 0.0

    if (Rectangle(2) > Rectangle(0)) {
      xmin = Rectangle(0)
      xmax = Rectangle(2)
    } else if (Rectangle(2) < Rectangle(0)) {
      xmin = Rectangle(2)
      xmax = Rectangle(0)
    }

    if (x >= xmin && x <= xmax && y >= ymin && y <= ymax)
      return true
    else
      return false
  }

}

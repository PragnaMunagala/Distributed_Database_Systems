package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(ST_Contains(queryRectangle,pointString)))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(ST_Contains(queryRectangle,pointString)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(ST_Within(pointString1,pointString2,distance)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(ST_Within(pointString1,pointString2,distance)))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def ST_Contains(rect:String, point:String): Boolean = {
    val rectangle_coordinates = rect.split(",")
    val target_point_coordinates = point.split(",")

    val point_x: Double = target_point_coordinates(0).trim.toDouble
    val point_y: Double = target_point_coordinates(1).trim.toDouble
    val rect_x1: Double = math.min(rectangle_coordinates(0).trim.toDouble, rectangle_coordinates(2).trim.toDouble)
    val rect_y1: Double = math.min(rectangle_coordinates(1).trim.toDouble, rectangle_coordinates(3).trim.toDouble)
    val rect_x2: Double = math.max(rectangle_coordinates(0).trim.toDouble, rectangle_coordinates(2).trim.toDouble)
    val rect_y2: Double = math.max(rectangle_coordinates(1).trim.toDouble, rectangle_coordinates(3).trim.toDouble)

    if ((point_x >= rect_x1) && (point_x <= rect_x2) && (point_y >= rect_y1) && (point_y <= rect_y2)) {
      return true
    }
    return false
  }

  def ST_Within(point1:String, point2:String, distance:Double): Boolean = {
    val point1_coordinates = point1.split(",")
    val point2_coordinates = point2.split(",")

    val point1_x: Double = point1_coordinates(0).trim.toDouble
    val point1_y: Double = point1_coordinates(1).trim.toDouble

    val point2_x: Double = point2_coordinates(0).trim.toDouble
    val point2_y: Double = point2_coordinates(1).trim.toDouble

    val x_dist = (point1_x-point2_x)*(point1_x-point2_x)
    val y_dist = (point1_y-point2_y)*(point1_y-point2_y)

    val dist_points = math.sqrt(x_dist + y_dist)

    if(dist_points <= distance) {
      return true
    }
    return false
  }
}

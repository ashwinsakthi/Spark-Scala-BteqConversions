package com.bteq

import java.io.File
import java.io.PrintWriter
import java.io.StringWriter
import org.apache.spark.sql.{ SaveMode, SparkSession }
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row

object BteqConvert_ImportScenario1 {

  def main(args: Array[String]) {

    val warehouseLocation = "spark-warehouse"

    val spark = SparkSession
      .builder()
      .appName("Bteq Converter BteqConvert_ImportScenario1 ")

      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport() // don't forget to enable hive support
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    try { // Emitting the error and stopping the application at the finally block.

      val skipValue = 1

      var limitValue = 0

      val delimValue = ","
      
      val repeatValue = "*"
            
      val df = spark.read.format("com.databricks.spark.csv").option("delimiter", delimValue).option("header", "true").load("/user/bigframe/ashwin/data1/datafile.txt")
      
      if(repeatValue == "*"){
        limitValue = df.count().toInt
      }else{
        limitValue = repeatValue.toInt
      }
      
      val df1 = df.rdd.zipWithIndex().filter(x => { x._2 > skipValue && x._2 <= limitValue }).map(_._1)      

      val schema = StructType(Array(StructField("empid",StringType,false),StructField("fname",StringType,false),StructField("lname",StringType,false),StructField("ph",StringType,false)))
      
      val result = spark.sqlContext.createDataFrame(df1, schema)
      result.show(false)
      
      result.createTempView("EmployeeTemp")      
      sql("select * from EmployeeTemp").show
      
      val finalResult = sql("select empid,fname,lname,ph from EmployeeTemp")      
      finalResult.write.mode(SaveMode.Append).insertInto("arka.importtest")
  
    } catch {
      case e: Exception => {
        println("The exception is executed")
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        println(sw.toString)
      }
    } finally { // Logging Off.
      println("Finally is executed. Stopping Spark Session")
      spark.stop()
    }

  }
}

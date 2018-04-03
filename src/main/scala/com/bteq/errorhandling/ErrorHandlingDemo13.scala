package com.bteq.errorhandling

import java.io.PrintWriter
import java.io.StringWriter
import org.apache.spark.sql.{ SaveMode, SparkSession }
import org.apache.spark.sql.AnalysisException

object ErrorHandlingDemo13 {
  
  def main(args: Array[String]){
    val warehouseLocation = "spark-warehouse"

    val spark = SparkSession
      .builder()
      .appName("Bteq Converter ErrorHandlingDemo13")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport() // don't forget to enable hive support
      .getOrCreate()

    var maxerror = 9999

    var errmsg = ""
    var errorSeverities = new ErrorSeverities()
    var errorSevMap = errorSeverities.errorSevMap
    errorSevMap(3534) = 8
    errorSevMap(3807) = 8
    errorSevMap(3804) = 8
    errorSevMap(5628) = 0
    
    var sevnum = 0

    import spark.implicits._
    import spark.sql
    
    def extractedWithReturnError(e: Exception):Int = {
      println("AnalysisException Occurred")
      errmsg = e.getMessage().trim()
      errmsg = errmsg.substring(0, errmsg.indexOf(";"))
      var errorCode = errorSeverities.matchErrorCode(errmsg)
      sevnum = errorSevMap.get(errorCode).getOrElse(0)

      if (sevnum > maxerror) {
        System.exit(1)
      }
      return errorCode
    }

    def extracted(e: Exception) = {
      println("AnalysisException Occurred")
      errmsg = e.getMessage().trim()
      errmsg = errmsg.substring(0, errmsg.indexOf(";"))

      sevnum = errorSevMap.get(errorSeverities.matchErrorCode(errmsg)).getOrElse(0)

      if (sevnum > maxerror) {
        System.exit(1)
      }
    }

    try {

      try { 
        
        val selectDf = sql("SELECT A.* FROM sample.APR_EMP A INNER JOIN sample.APRUPD_EMP_DTL B ON A.EMP_ID=B.EMP_ID WHERE A.emp_AGE = CASE WHEN A.EMP_AGE > 30 THEN 33 WHEN A.emp_AGe < 30 THEN 26 END")
        selectDf.show()
        selectDf.write.mode(SaveMode.Append).insertInto("sample.APRUPD_EMP")       

      } catch { // Emitting the error and stopping the application at the finally block.
        case e: Exception => {
          extracted(e)
          
        }
      }
      
      if (sevnum == 0) {
            L1()
      }
      
      if (sevnum >= 4) {
            L2()
      }

      def L1() {
        try {
          sql("select emp_id,emp_status from bigm.apr_emp").show()
          System.exit(0)
        } catch {
          case e: AnalysisException =>
            {
              extracted(e)
            }
        }
      }

      def L2() {
        try {
          sql("select emp_id,emp_name,emp_status from bigm.apr_emp").show()
        } catch {
          case e: AnalysisException =>
            {
              extracted(e)
            }
        }
      }     

    } catch {
      case ex: Exception => {
        println("The exception is executed message " + ex.getMessage)
        val sw = new StringWriter
        ex.printStackTrace(new PrintWriter(sw))
        println(sw.toString)
      }
    } finally { // Logging Off.
      println("Finally is executed. Stopping Spark Session")
      spark.stop()
    }
  }
  
}

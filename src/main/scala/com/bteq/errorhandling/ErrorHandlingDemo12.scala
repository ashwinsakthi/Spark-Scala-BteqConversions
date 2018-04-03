package com.bteq.errorhandling

import java.io.PrintWriter
import java.io.StringWriter
import org.apache.spark.sql.{ SaveMode, SparkSession }
import org.apache.spark.sql.AnalysisException

object ErrorHandlingDemo12 {
  
  def main(args: Array[String]){
    val warehouseLocation = "spark-warehouse"

    val spark = SparkSession
      .builder()
      .appName("Bteq Converter ErrorHandlingDemo12")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport() // don't forget to enable hive support
      .getOrCreate()

    var maxerror = 8

    var errmsg = ""
    var errorSeverities = new ErrorSeverities()
    var errorSevMap = errorSeverities.errorSevMap
       
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
        
        sql("DROP TABLE IF EXISTS sample.participant_events_stats")             

      } catch {
        case e: Exception => {
          extracted(e)          
        }
      }
      
      if (sevnum == 8) {
            xyz()
      }
      maxerror=7
      errorSevMap(5628) = 0
      
      try { 
        
        sql("select count(*) from sample.apr_emp")             

      } catch {
        case e: Exception => {
          extracted(e)          
        }
      }
      
       errorSevMap(5628) = 8
      
       try { 
        
        sql("select count(*) from sample.apr_emp")             

      } catch {
        case e: Exception => {
          extracted(e)          
        }
      }

      def xyz() {
        try {
           maxerror=3
           errorSevMap(3803) = 0
          sql("create table sample.participants_events (id int, eid int)").show()
          
        } catch {
          case e: AnalysisException =>
            {
              extracted(e)
            }
        }
      }   
      
      try { 
        
        sql("select count(1) from sample.participants_events")             

      } catch {
        case e: Exception => {
          extracted(e)          
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

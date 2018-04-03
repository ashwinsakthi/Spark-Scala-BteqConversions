package com.bteq.errorhandling

import java.io.PrintWriter
import java.io.StringWriter
import org.apache.spark.sql.{ SaveMode, SparkSession }
import org.apache.spark.sql.AnalysisException

object ErrorHandlingDemo11 {
  
  def main(args: Array[String]){
    val warehouseLocation = "spark-warehouse"

    val spark = SparkSession
      .builder()
      .appName("Bteq Converter ErrorHandlingDemo11")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport() // don't forget to enable hive support
      .getOrCreate()

    var maxerror = 9999

    var errmsg = ""
    var errorSeverities = new ErrorSeverities()
    var errorSevMap = errorSeverities.errorSevMap

    errorSevMap(3807) = 0
    
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
        
         sql("DROP TABLE IF EXISTS bigm.MyTable1")  
         
      } catch { // Emitting the error and stopping the application at the finally block.
        case e: Exception => {
          extracted(e)
          
        }
      }
      
      errorSevMap(3807) = 8
      
      try { 
        
         sql("CREATE TABLE bigm.MyTable1(id INTEGER ,val INTEGER) PRIMARY INDEX (id)")  
         
      } catch { // Emitting the error and stopping the application at the finally block.
        case e: Exception => {
          var errorCode =extractedWithReturnError(e)
          if (errorCode !=0){
            System.exit(errorCode)
          }
          
        }
      }
      
       try { 
        
        val activityCount = sql("select 1 from bigm.mytable1 where id = 1 group by 1").count()
        
        if(activityCount!=1){
          L1()
        }else if(activityCount==0){
          System.exit(0)
        }else if(activityCount>=2){
          System.exit(99)
        }
         
      } catch { // Emitting the error and stopping the application at the finally block.
        case e: Exception => {
          extracted(e)
          
        }
      }
      
      try { 
        
          
        val activityCount = sql("select 1 from bigm.mytable1 where id = 2 group by 1").count()
        
        if(activityCount==1){
          L2()
        }
         
      } catch { // Emitting the error and stopping the application at the finally block.
        case e: Exception => {
          var errorCode =extractedWithReturnError(e)
          if (errorCode !=0){
            System.exit(errorCode)
          }
          
        }
      }
      
       try {
          val updatedDf2 = sql("select 2 id ,2 val from bigm.mytable1")

            updatedDf2.show()
            updatedDf2.write.mode(SaveMode.Overwrite).insertInto("bigm.mytable1")

        } catch {
          case e: AnalysisException =>
            {
              var errorCode =extractedWithReturnError(e)
              if (errorCode !=0){
                System.exit(errorCode)
              }
            }
        }
      
        
       val repeatValue = 2
       
       for( a <- 1 to repeatValue ){  
        repeatMethod()  
       }  

      def repeatMethod() {

        try {
          val updatedDf2 = sql("select * from bigm.mytable1")
          
          print("hello")
        } catch {
          case e: AnalysisException =>
            {
              var errorCode = extractedWithReturnError(e)
              if (errorCode != 0) {
                System.exit(errorCode)
              }
            }
        }
      }
       
        
              def L1() {
                try {
                  val updatedDf2 = sql("select 1 id ,1 val from bigm.mytable1")
        
                  updatedDf2.show()
                  updatedDf2.write.mode(SaveMode.Overwrite).insertInto("bigm.mytable1")
                  print("L1")
                } catch {
                  case e: AnalysisException =>
                    {
                      var errorCode = extractedWithReturnError(e)
                      if (errorCode != 0) {
                        System.exit(errorCode)
                      }
                    }
                }
              }
        
              def L2() {
                try {
                  print("L2")
                  val activityCount = sql("select 1 from bigm.mytable1 where id = 3 group by 1").count()
                  if (activityCount == 0) {
                    L4()
                  }
        
                  if (activityCount == 1) {
                    L3()
                  }
        
                } catch {
                  case e: AnalysisException =>
                    {
                      extracted(e)
                    }
                }
              }
        
              def L3() {
                try {
                  //sql("select 'L3' from bigm.mytable1")
                   print("L3")
                } catch {
                  case e: AnalysisException =>
                    {
                      extracted(e)
                    }
                }
              }
        
              def L4() {
                try {
                  //sql("select 1 from bigm.mytable1")
                  print("L4")
                } catch {
                  case e: AnalysisException =>
                    {
                      var errorCode = extractedWithReturnError(e)
                      if (errorCode != 0) {
                        System.exit(errorCode)
                      }
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

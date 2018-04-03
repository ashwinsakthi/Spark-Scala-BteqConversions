package com.custom.objects

import java.io.PrintWriter
import java.io.StringWriter
import org.apache.spark.sql.{ SaveMode, SparkSession }
import com.bteq.errorhandling.ErrorSeverities
import org.apache.spark.sql.AnalysisException

object BteqConvert_BteqErrorGoTo {

  def main(args: Array[String]){
    val warehouseLocation = "spark-warehouse"

    val spark = SparkSession
      .builder()
      .appName("Bteq Converter BteqConvert_BteqErrorGoTo")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport() // don't forget to enable hive support
      .getOrCreate()

    var maxerror = 9999

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

      try { // Emitting the error and stopping the application at the finally block.
        val selectDf = sql("Select from_unixtime(unix_timestamp()) run_time ")
        selectDf.show()
        

      } catch { // Emitting the error and stopping the application at the finally block.
        case e: Exception => {
          println("AnalysisException Occurred")
          errmsg = e.getMessage().trim()
          errmsg = errmsg.substring(0, errmsg.indexOf(";"))
          sevnum = errorSevMap.get(errorSeverities.matchErrorCode(errmsg)).getOrElse(0)

          if (sevnum > maxerror) {
            System.exit(1)
          }
          
        //.IF ERRORCODE <>0 THEN quit
          //.IF ERRORCODE <> 0 THEN .GOTO UNUSUAL_LBL;
            if(errmsg!=null && errmsg.trim().length()==0){
               UNUSUAL_LBL()
            }

        }
      }

      errorSevMap(3803) = 889
      maxerror = 1536

      try {

        sql("create table bigm.xxx (eid int , name string)").show()

      } catch {
        case e: Exception => 
          
          extracted(e)

          ERR_LBL()
          
      }

      errorSevMap(5682) = 29

      try {

        sql("select emp_id ,emp_name1 from bigm.apr_emp").show()

      } catch {
        case e: Exception => {
          
          var errorCode =extracted(e)

          if (errorCode == 3803) {
            ERR_LBL1()
          } else if (errorCode == 3804) {
            ERR_LBL2()
          }
        }
      }

      maxerror = 2

      try {

        sql("select * from bigm.apr_emp_dtl").show()

      } catch {
        case e: Exception => {
          println("AnalysisException Occurred")
          //.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
          System.exit(1)
          /* if(errmsg!=null && errmsg.trim().length()==0){
               UNUSUAL_LBL()
            }*/
        }
      }

      errorSevMap(5628) = 0

      try {

        sql("select a.emp_id , a.emp_name,b.emp_dgn,b.emp_exp1 from bigm.apr_emp a join bigm.apr_emp_dtl1 b on a.emp_id = b.emp_id").show()

      } catch {
        case e: Exception => {
          println("AnalysisException Occurred")
          errmsg = e.getMessage().trim()
          errmsg = errmsg.substring(0, errmsg.indexOf(";"))

          sevnum = errorSevMap.get(errorSeverities.matchErrorCode(errmsg)).getOrElse(0)

          if (sevnum > maxerror) {
            System.exit(1)
          }

          CHKLBL1()

        }
      }

      try {

        val recCnt = sql("select col1 from bigm.apr_emp_dgn").count()
        if (recCnt > 0) {
          UNUSUAL_LBL()
        }
      } catch {
        case e: Exception => {
          println("AnalysisException Occurred")
          errmsg = e.getMessage().trim()
          errmsg = errmsg.substring(0, errmsg.indexOf(";"))

          sevnum = errorSevMap.get(errorSeverities.matchErrorCode(errmsg)).getOrElse(0)

          if (sevnum > maxerror) {
            System.exit(1)
          }
        }
      }
      
      UNUSUAL_LBL()
      ERR_LBL()
      ERR_LBL1()
      ERR_LBL2()
      CHKLBL1()
      
      
      
      //Need to discuss here -- 
      /**
       * .IF ACTIVITYCOUNT <> 0 THEN .GOTO CHKLBL3; 
select * from bigm.apr_emp_dgn;
       * 
       */
      CHKLBL2()
      CHKLBL3()

      def UNUSUAL_LBL() {

        try {
          print("Very Unusual")
          sql("select * from bigm.apr_emp").show()

        } catch {
          case e: AnalysisException =>
            {
              errmsg = e.getMessage().trim()
              errmsg = errmsg.substring(0, errmsg.indexOf(";"))
              sevnum = errorSevMap.get(errorSeverities.matchErrorCode(errmsg)).getOrElse(0)

              if (sevnum > maxerror) { System.exit(1) }
            }
        }

      }

      def ERR_LBL() {

        try {
          print("Table has not created")
          sql("select * from bigm.apr_emp_dtl").show()

        } catch {
          case e: AnalysisException =>
            {
              errmsg = e.getMessage().trim()
              errmsg = errmsg.substring(0, errmsg.indexOf(";"))
              sevnum = errorSevMap.get(errorSeverities.matchErrorCode(errmsg)).getOrElse(0)
              if (sevnum > maxerror) { System.exit(1) }
            }
        }

      }

      def ERR_LBL1() {

        try {
          print("duplicate table")
          sql("select * from bigm.apr_emp").show()

        } catch {
          case e: AnalysisException =>
            {
              errmsg = e.getMessage().trim()
              errmsg = errmsg.substring(0, errmsg.indexOf(";"))
              sevnum = errorSevMap.get(errorSeverities.matchErrorCode(errmsg)).getOrElse(0)
              if (sevnum > maxerror) { System.exit(1) }
            }
        }

      }

      def ERR_LBL2() {

        try {
          print("duplicate view")
          sql("select * from bigm.apr_emp").show()

        } catch {
          case e: AnalysisException =>
            {
              errmsg = e.getMessage().trim()
              errmsg = errmsg.substring(0, errmsg.indexOf(";"))
              sevnum = errorSevMap.get(errorSeverities.matchErrorCode(errmsg)).getOrElse(0)
              if (sevnum > maxerror) { System.exit(1) }
            }
        }

      }

      def CHKLBL1() {
        try {
          val recCnt =  sql("select b.emp_dgn,a.emp_dept,a.emp_exp,a.emp_id from bigm.apr_emp_dgn b inner join bigm.apr_emp_dtl a on a.emp_dgn = b.emp_dgn where a.emp_exp > 3").count()
         /* if(recCnt!=0){
            CHKLBL3()
          }*/
        } catch {
          case e: AnalysisException =>
            {
              errmsg = e.getMessage().trim()
              errmsg = errmsg.substring(0, errmsg.indexOf(";"))
              sevnum = errorSevMap.get(errorSeverities.matchErrorCode(errmsg)).getOrElse(0)
              if (sevnum > maxerror) { System.exit(1) }
            }
        }
      }

      def CHKLBL2() {
        try {
          print("No record in the table")

        } catch {
          case e: AnalysisException =>
            {
              errmsg = e.getMessage().trim()
              errmsg = errmsg.substring(0, errmsg.indexOf(";"))
              sevnum = errorSevMap.get(errorSeverities.matchErrorCode(errmsg)).getOrElse(0)
              if (sevnum > maxerror) { System.exit(1) }
            }
        }
      }

      def CHKLBL3() {
        try {

          val selectDf = sql("select count(*) from bigm.apr_emp_dgn where emp_exp > 3")
        } catch {
          case e: AnalysisException =>
            {
              errmsg = e.getMessage().trim()
              errmsg = errmsg.substring(0, errmsg.indexOf(";"))
              sevnum = errorSevMap.get(errorSeverities.matchErrorCode(errmsg)).getOrElse(0)
              if (sevnum > maxerror) { System.exit(1) }
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

package com.bteq
import java.io.File
import java.io.PrintWriter
import java.io.StringWriter
import org.apache.spark.sql.{ SaveMode, SparkSession }

object BteqConvert {
  
  //Conversion for Sample_bteq1.sql

  def main(args: Array[String]) {

    //val warehouseLocation = new File("/apps/hive/warehouse").getAbsolutePath
    val warehouseLocation = "spark-warehouse"

    val spark = SparkSession
      .builder()
      .appName("Bteq Converter")
      //.config("hive.metastore.uris", "thrift://localhost:9083") // replace with your hivemetastore service's thrift url
      //"spark.sql.warehouse.dir"
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport() // don't forget to enable hive support
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    val sqlDF = sql("SELECT DISTINCT * FROM bigm_ashwin.APR_EMP A INNER JOIN bigm_ashwin.APR_EMP_dtl B ON A.EMP_ID = B.EMP_ID Where emp_AGE = CASE WHEN EMP_AGE > 30 THEN 35 WHEN emp_AGe < 30 THEN 26 END order by EMP_SAL DESC")

    sqlDF.show()

    try { // Emitting the error and stopping the application at the finally block.

      val sqlJoinDF = sql("SELECT A.* FROM bigm_ashwin.APR_EMP A RIGHT OUTER JOIN bigm_ashwin.APR_EMP_DTL B ON A.EMP_ID=B.EMP_ID WHERE A.emp_AGE = CASE WHEN A.EMP_AGE > 30 THEN 33 WHEN A.emp_AGe < 30 THEN 26 END")
      
    /*
     * org.apache.spark.sql.AnalysisException: `bigm_ashwin`.`aprupd_emp` requires
     *  that the data to be inserted have the same number of columns as 
     *  the target table: target table has 10 column(s) but the inserted data 
     *  has 2 column(s), including 0 partition column(s) having constant value(s).; * 
     * 
     */
      sqlJoinDF.show()

      //sqlJoinDF.write.mode(SaveMode.Append).saveAsTable("bigm_ashwin.APRUPD_EMP")
      sqlJoinDF.write.mode(SaveMode.Append).insertInto("bigm_ashwin.APRUPD_EMP")

      if (sqlJoinDF.count() == 0) {
        // Goto REGULAR_INSERT
        val regularInsertDF = sql("SELECT A.* FROM bigm_ashwin.APR_EMP A")
        
        regularInsertDF.show()

        regularInsertDF.write.mode(SaveMode.Append).insertInto("bigm_ashwin.APRUPD_EMP")
        if (regularInsertDF.count() == 0) {
          return ;
        }
      }

      // Goto Cleanup
      val cleanUpDF = sql("SELECT * FROM bigm_ashwin.APRUPD_EMP_TMP where emp_id=2000").distinct()
      cleanUpDF.show();
      cleanUpDF.write.mode(SaveMode.Append).insertInto("bigm_ashwin.APRUPD_EMP")

    } catch { // Emitting the error and stopping the application at the finally block.
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

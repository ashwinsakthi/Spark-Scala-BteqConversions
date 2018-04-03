package com.custom.objects

object ErrorTest {

  def matchError(errorString: String): Int = errorString match {
    
      case "Table Not Found" =>
        println("Table Not Found")
        return 5
        
      case "Column Not Found" =>
        println("Column Not Found")
        return 7
    }
  
   def matchError(errorString: String,modifiedSeverity: Int): Int = errorString match {
    
      case "Table Not Found" =>
        println("Table Not Found")
        println(modifiedSeverity)
        return modifiedSeverity
        
      case "Column Not Found" =>
        println("Column Not Found")
        println(modifiedSeverity)
        return modifiedSeverity
    }
   
   def main(args: Array[String]) {
   
     println("matchError")
     matchError("Table Not Found")
     
     println("matchError with modifiedSeverity")
     matchError("Table Not Found",11)
  
   
   }
   
}

package com.bteq.errorhandling

object TestRegex {
  val string123 = s"Table or view not found"
  
  def main(args: Array[String]) {
    print(new ErrorSeverities().matchErrorCode("cannot resolve '`asdasd`' given input columns: "))
    //val pattern = ".*Table or view not found.*".r
    //val str = "Table or view not found"
      
    //  println(pattern findFirstIn str)
  }
}

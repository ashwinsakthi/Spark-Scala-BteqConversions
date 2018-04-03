package com.bteq.errorhandling

class ErrorSeverities {
  
  /*val tableViewNotFoundRegex = ".*Table or view not found.*".r
  
  val columnNotFoundRegex = ".*cannot resolve.*given input columns.*".r
  
    var errorSevMap = collection.mutable.Map[Int, Int]()
        errorSevMap(3802)=8
        errorSevMap(5628)=8  
        errorSevMap(9999)=8
             
   def matchErrorCode(errorString: String): Int =
   errorString match {    
      case tableViewNotFoundRegex() =>
        println("Table Not Found")
        return 3802        
      case columnNotFoundRegex() =>
        println("Column Not Found")
        return 5628
      case default =>
        return 9999
   }*/
  
  val tableViewNotFoundRegex = ".*Table or view not found.*".r
  val duplicateColumnRegex=".*Found duplicate column.*".r
  val columnNotFoundRegex = ".*cannot resolve.*given input columns.*".r
  val TableExistRegex=".*Table or view.*already exists.*".r
  val ViewExistRegex=".*View.*already exists.*".r
  val ColumnZeroLengthRegex=".*Varchar length 0 out of allowed range.*".r
  
  var errorSevMap = collection.mutable.Map[Int, Int]()
        errorSevMap(3802)=8
        errorSevMap(5628)=8
        errorSevMap(9999)=8
        errorSevMap(3803)=8
        errorSevMap(3804)=8
        errorSevMap(3515)=8

   def matchErrorCode(errorString: String): Int =
   errorString match {
      case tableViewNotFoundRegex() =>
        println("Table Not Found")
        return 3802
     case columnNotFoundRegex() =>
        println("Column Not Found")
        return 5628
    case duplicateColumnRegex() =>
        println("DuplicateColumnFound")
        return 3515
    case TableExistRegex() =>
        println("Table already Exist")
        return 3803
   case ViewExistRegex()=>
        println("View already Exist")
        return 3804
   case ColumnZeroLengthRegex()=>
        println("Column Length Cannnot be Zero")
        return 5325
     case default =>
        return 9999
   }
  
}

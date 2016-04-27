package net.datagraft.sparker.util

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import java.sql.Date
import java.text.SimpleDateFormat


/**
  * Created by nive on 4/18/2016.
  */
object UtilityFunctions {

  def getUtilityUDF(funcStr : String): Column ={
    val udfFunction = funcStr.trim match {
      case "add-file-name-column" => input_file_name()
      case "current-date" => current_date()
      case "current-timestamp" => current_timestamp()
    }
    udfFunction
  }

  def getUtilityUDF(funcStr : String, value: Any): Column ={
    val udfFunction = funcStr.trim match {
      case "with-custom-value" => lit(value)
    }
    udfFunction
  }

  val toDate = udf( (dob: String , format: String) => {
    val javaUtilDate = new SimpleDateFormat(format).parse(dob)
    new Date(javaUtilDate.getTime())
  })


  def getApplyFunctionForColumn(colName: String, funcStr: List[String]) : Column = {
    val udfFunction = funcStr.head.trim match {
      //replace
      case "replace" => regexp_replace(col(colName) ,funcStr(1), funcStr(2))
      //toLowerCase
      case "tolowercase" => lower(col(colName))
      //Capitalise
      case "touppercase" => upper(col(colName))
      //Cast
      case "cast" => col(colName).cast(ScalableGrafterInterOpHelper.getFieldTypeInSchema(funcStr(1)))
      // lit with given value
      //parse eu date
      case "date" => toDate(col(colName), lit(funcStr(1)))
      //remove blanks
      case "trim" => trim(col(colName))
      //inc
      case "increment" => col(colName)+1
      //add
      case "inc" => col(colName)+funcStr(1).toDouble
      //dec
      case "dec" => col(colName)-funcStr(1).toDouble
      //multipy
      case "multipy" => col(colName)*funcStr(1).toDouble
      //divide
      case "divide" => col(colName)/funcStr(1).toDouble
      //reminder
      case "reminder" => col(colName)%funcStr(1).toDouble
      //factorial
      case "factorial" => factorial(col(colName).cast(ScalableGrafterInterOpHelper.getFieldTypeInSchema("int")))
      //absolute
      case "abs" => abs(col(colName).cast(ScalableGrafterInterOpHelper.getFieldTypeInSchema("int")))
      //power
      case "power" => pow(col(colName),funcStr(1).toInt)
      //substring
      case "substr" => substring(col(colName),funcStr(1).toInt, funcStr(2).toInt)


//      case "to-eu-date" => to_date(col(colName))
      //parse us date
      //parce long lang

    }
    udfFunction
  }

  def getDerivingUDF(colList : List[String], funcStr: String) : Column={
    val udfFunc = funcStr.trim.toLowerCase match {
      case "max" =>max(colList.head)
      case "min" =>min(colList.head)
      case "avg" =>avg(colList.head)
      case "sum" =>sum(colList.head)
      case "count" =>count(colList.head)
    }
    udfFunc
  }

  def getFilterExp(colToApply:String, funcStr: String, exprStrToApply: String) : Column = {
    val udfFunc = funcStr.trim.toLowerCase match {
      case "match" =>col(colToApply).like(exprStrToApply)
      case "endswith" =>col(colToApply).endsWith(exprStrToApply)
      case "startswith" =>col(colToApply).startsWith(exprStrToApply)
      case "contains" =>col(colToApply).contains(exprStrToApply)
      case "regex" =>col(colToApply).rlike(exprStrToApply)
      case "notmatch" => col(colToApply).!==(exprStrToApply)
      case ">" => col(colToApply).>(exprStrToApply)
      case ">=" => col(colToApply).>=(exprStrToApply)
      case "<=" => col(colToApply).<=(exprStrToApply)
      case "<" => col(colToApply).<(exprStrToApply)

//      case "+" => col(colToApply).cast(DoubleType).+(exprStrToApply)


    }
    udfFunc
  }

}

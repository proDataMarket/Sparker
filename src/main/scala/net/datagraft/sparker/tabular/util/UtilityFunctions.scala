package net.datagraft.sparker.tabular.util

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.IntegerType
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
      //toLowerCase
      case "lower-case" => lower(col(colName))
      //touppercase
      case "upper-case" => upper(col(colName))
        //capitalise
      case "capitalize" => initcap(col(colName))
      //toLowerCase
      case "reverse" => reverse(col(colName))
      //remove blanks
      case "trim" => trim(col(colName))
      //trimn
      case "trim-newline" => regexp_replace(col(colName) ,"\n", "")
      //Removes whitespace from the left side of string
      case "triml" => ltrim(col(colName))
      //trimn
      case "trimr" => rtrim(col(colName))
      //inc by 1
      case "inc" => col(colName).cast(ScalableGrafterInterOpHelper.getFieldTypeInSchema("int"))+1
      //dec by 1
      case "dec" => col(colName).cast(ScalableGrafterInterOpHelper.getFieldTypeInSchema("int"))-1
      //add
      case "add" => col(colName)+funcStr(1).toDouble
      //add
//      case "join" => col(colName)+funcStr(1).toDouble
      //replace
      case "replace" => regexp_replace(col(colName) ,funcStr(1), funcStr(2))
      //Cast
      case "cast" => col(colName).cast(ScalableGrafterInterOpHelper.getFieldTypeInSchema(funcStr(1)))
      // lit with given value
      //parse eu date
      case "fr" => toDate(col(colName), lit(funcStr(1)))
      //dec
      case "subtract" => col(colName)-funcStr(1).toDouble
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
      //prefixer
      case "prefixer" => concat(List(lit(funcStr(1)), col(colName)): _*)
        //fillemptywith
      case "fill-empty-with" => regexp_replace(col(colName) ,"", funcStr(1))

      case "max" =>max(colName)
      case "min" =>min(colName)
      case "avg" =>avg(colName)
      case "sum" =>sum(colName)
      case "count" =>count(colName)


//      case "to-eu-date" => to_date(col(colName))
      //parse us date
      //parce long lang

    }
    udfFunction
  }


  def getFilterExp(colToApply:String, funcStr: String, expr: List[String]) : Column = {
    val exprStrToApply = expr(1);
    var udfFunc = funcStr.trim.toLowerCase match {
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
      case "not-empty?" => col(colToApply).isNotNull.and(col(colToApply).!==(""))


//      case "+" => col(colToApply).cast(DoubleType).+(exprStrToApply)


    }
    if(expr(0).equalsIgnoreCase("drop")){
      udfFunc= not(udfFunc)
    }
    udfFunc
  }

}

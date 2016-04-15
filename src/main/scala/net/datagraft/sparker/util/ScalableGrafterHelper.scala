package net.datagraft.sparker.util

import org.apache.spark.sql.types._


/**
  * Created by nive on 4/14/2016.
  */
object ScalableGrafterHelper {

  def getFieldTypeInSchema(fieltType: String): DataType = {
    val dataType = fieltType.toLowerCase.trim match {
      case "num" => DoubleType
      case "alpha" =>  StringType
      case "len" =>  IntegerType
      case "date" =>  TimestampType
      case "int" =>  IntegerType
      case "double" =>  DoubleType
      case "long" =>  LongType
      case "float" =>  FloatType
      case "byte" =>  ByteType
      case "string" =>  StringType
      case "timestamp" =>  StringType
      case "uuid" =>  StringType
      case "decimal" =>  DoubleType
      case "boolean" => BooleanType
      case "counter" => IntegerType
      case "bigint" => IntegerType
      case "text" =>  StringType
      case "ascii" =>  StringType
      case "varchar" =>  StringType
      case "varint" =>  IntegerType
      case _  => StringType  // the default, catch-all
    }
    dataType
  }


}

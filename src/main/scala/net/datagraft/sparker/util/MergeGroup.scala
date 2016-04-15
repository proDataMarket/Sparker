package net.datagraft.sparker.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{StringType, StructField, StructType, DataType}



/**
  * Custom User Defined Aggregate Function to merge values of grouped data with a separator
  * The column values are assumed to be String
  *
  * Created by nive on 4/14/2016.
  */
class MergeGroup(colName: String, separator: String) extends UserDefinedAggregateFunction{
  override def inputSchema: StructType =  StructType( StructField(colName,StringType) :: Nil)

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(buffer.getAs[String](0).isEmpty && !input.getAs[String](0).isEmpty){
      buffer(0) =  input.getAs[String](0)
    }
    else if( !buffer.getAs[String](0).isEmpty && !input.getAs[String](0).isEmpty){
      buffer(0) = buffer.getAs[String](0)+ separator + input.getAs[String](0)
    }
  }

  override def bufferSchema: StructType = StructType(
    StructField(colName+"Merge", StringType) :: Nil
  )

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if(buffer1.getAs[String](0).isEmpty && !buffer2.getAs[String](0).isEmpty){
      buffer1(0) =  buffer2.getAs[String](0)
    }
    else if( !buffer1.getAs[String](0).isEmpty && !buffer2.getAs[String](0).isEmpty){
      buffer1(0) = buffer1.getAs[String](0)+ separator + buffer2.getAs[String](0)
    }
  }

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  override def deterministic: Boolean = true

  override def evaluate(buffer: Row): Any = {
    buffer.getAs[String](0)
  }

  override def dataType: DataType = StringType
}

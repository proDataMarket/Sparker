package net.datagraft.sparker.core

import net.datagraft.sparker.util.{ScalableGrafterHelper, MergeGroup}
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * Main transformation class where basic transformations are performed on-top of DataFrame
  * Created by nive on 4/5/2016.
  */
class Transformations(sparkCont: SparkContext) {
  def defaultPageSize =50

  val sqlContext = new SQLContext(sparkCont)
  def makeDataSet(dataPath: String): DataFrame = {
     sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
//      .option("inferSchema", "true")
      .load(dataPath)
  }

  /**
    * Makes the first row of df as the column headers
    * @param df
    * @return df
    */
  def makeDataSetWithColumn(df: DataFrame): DataFrame = {
    val rdd = df.rdd.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val struct = StructType(
      df.first().mkString(",").split(",").map(StructField(_, StringType, true)))
    sqlContext.createDataFrame(rdd, struct)
  }

  def makeDataSet(df: DataFrame, cols: List[String]): DataFrame = {
    df.select(cols.head, cols.tail: _*)
  }

  def makeDataSet(df: DataFrame, to: Int, from: Int = 0): DataFrame = {
    // default of from is 0 (to merge easily with UI, also can extend feature to support *to*)
    val cols = df.columns.slice(from, to)
    df.select(cols.head, cols.tail: _*)
  }

//  def paginateDataFrame (df : DataFrame , pageNumber :Int,  pageSize :Int = defaultPageSize) : DataFrame = {
//    if( pageNumber.!=(0))
//      {
//        println( " pageing "+ pageNumber)
//        val firstPortion =df.limit(pageSize*pageNumber)
//        val page = df.repartition(1).except(firstPortion).limit(pageSize)
//        page
//      }
//    else{
//      df
//    }
//
//
//  }
  def getColumns(df : DataFrame) : String = {
    df.columns.mkString(",")
  }

  def getRowColumnMap (df : DataFrame) : String ={
    val cols = df.columns
    val rowMappedToColumns = df.map( f => f.getValuesMap(cols)).map(_.mkString(",").replace(" -> "," "))
    rowMappedToColumns.collect().mkString("({" , "} {", "})")
  }

  //Usage agg functions to be in a string map grpcol as a list val result = groupAndAggregate(df, Map("COMUNE"-> "COUNT"), listOfStrings)
  def groupAndAggregate(df: DataFrame, cols: List[String], aggregatecol : List[String], aggregateFun : List[String]): DataFrame = {

    val mapify = aggregatecol.zip(aggregateFun).toMap
    df.groupBy(cols.head, cols.tail: _*).agg(mapify)
  }

  // overloaded one to support merge UADF with below use cases
  //    val cols = List("name", "sex")
  //    val aggcol = List("street:agg" , "age:agg" , "street:first")
//  df = transformer.groupAndAggregate(df, cols, aggcol)
   def groupAndAggregate(df: DataFrame, cols: List[String], aggColFunExpr : List[String]/*, aggregateFun : List[String]*//*, aggregateFun : List[String]*/): DataFrame = {

    val mutableAggFunMap = scala.collection.mutable.LinkedHashMap[String, String]()
    for(colAndFun <-aggColFunExpr)
    {
      if(colAndFun.matches(".*:agg")){
        val col = colAndFun.split(":")(0)
        sqlContext.udf.register("merge",new MergeGroup(col, ":")) // for the time being merge separator is set to ":" TO be changed
        mutableAggFunMap += (col -> "merge")
      }
      else{
        val spilited = colAndFun.split(":")
        mutableAggFunMap.put(spilited(0), spilited(1))
      }
    }
    val immutableMap = mutableAggFunMap.toMap
    df.groupBy(cols.head, cols.tail: _*).agg(immutableMap)
  }

  def removeDuplicates(df: DataFrame): DataFrame = {
    df.distinct()
  }

  def removeDuplicates(df: DataFrame, cols: List[String]): DataFrame = {
//    val selectFirstValueOfNoneGroupedColumns = df.columns.filterNot(cols.toSet).map(_ -> "first").toMap
//    val grouped = df.groupBy(cols.head, cols.tail: _*).agg(selectFirstValueOfNoneGroupedColumns)
    df.dropDuplicates(cols)
  }

  def pivotDataSet(df: DataFrame, groupBy: List[String], pivotCol: String, values: List[String]) = {
    df.groupBy(groupBy.head, groupBy.tail: _*).pivot(pivotCol, values)
  }


  def sortDataSetWithColumnExpr(df : DataFrame , columnsToSort: List[String], sortingExprStr: List[String]) : DataFrame = {
    val listOfColumn = scala.collection.mutable.ListBuffer[Column]()
    var dfUpdated = df
    for( (colName, sortingExprForCol) <- columnsToSort zip sortingExprStr){

      val splits = sortingExprForCol.split(":")
      dfUpdated = dfUpdated.withColumn(colName, dfUpdated.col(colName).cast(ScalableGrafterHelper.getFieldTypeInSchema(splits(1))))
      if(splits(0)=="desc") listOfColumn += col(colName).desc
      else if(splits(0)=="asc") listOfColumn += col(colName).asc
    }
    dfUpdated.sort(listOfColumn.toList: _*)
  }


  // http://blog.cloudera.com/blo
  // g/2015/03/how-to-tune-your-apache-spark-jobs-part-1/
  // need to check for improved sorting
  // syntax column , type asc/desc
  //  def sortDataSet(df : DataFrame, groupFunctions: Map[String, String]) : DataFrame = {
  //    val cols = List[org.apache.spark.sql.Column]
  //    groupFunctions foreach (x => df.col(x._1).cast(IntegerType).desc)
  //    df.sort()
  //  }

  def addColumn(df: DataFrame, columns: List[String]): DataFrame = {
    df
  }

  def selectColumn(df: DataFrame, from: Int, to: Int): DataFrame = {
    df
  }

  def renameColumn(df: DataFrame, existingName: String, newName: String): DataFrame = {
    df.withColumnRenamed(existingName, newName)
  }

  def mergeColumn(df: DataFrame): DataFrame = {
    df
  }

  def show(df: DataFrame): DataFrame = {
    df.show()
    df
  }

  /**
    * Creates a csv file of given DataFrame. Overwrites if anything already exist in provided output path
    *
    * @todo Make it without repartition and merge them to new file and return the new file location as output
    * @todo analyse for any better solutions in future
    * @param df
    * @param filePath
    */
  def saveDataAsCsv(df: DataFrame, filePath: String): String = {
    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val hdfs = FileSystem.get(hadoopConf)
    val path = new Path(filePath)

    val mergedPath = "merged-"+filePath+".csv"
    val merged = new Path (mergedPath)
        if (hdfs.exists(merged)) {
          hdfs.delete(merged, true)
        }
    df.write
      .format("com.databricks.spark.csv")
//      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(filePath)

    FileUtil.copyMerge(hdfs, path, hdfs, merged, false, hadoopConf, null)
    mergedPath
  }



}

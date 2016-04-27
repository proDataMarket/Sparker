package net.datagraft.sparker.core

import net.datagraft.sparker.util.{UtilityFunctions, ScalableGrafterInterOpHelper, MergeGroup}
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * Created by nive on 4/5/2016.
  */
class Transformations(sparkCont: SparkContext) {
  def defaultPageSize =50

  val sqlContext = new SQLContext(sparkCont)
  var filename = ""

  def makeDataSet(dataPath: String): DataFrame = {
    this.filename = dataPath
     sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("treatEmptyValuesAsNulls", "true" )
      .option("parserLib", "univocity")
      .load(dataPath)
//       .na.fill("NA")
  }

  def makeDataSet(dataPath: String , doSample: Boolean, sampleLimit: Int): DataFrame = {
    this.filename = dataPath
    var df =sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("treatEmptyValuesAsNulls", "true" )
      .option("parserLib", "univocity")
      .load(dataPath)
//      .na.fill("NA")
    if(doSample){//need a better mechanism to decide on samples size
      val dfsample =df.sample(true, 0.1)
      if(dfsample.count()>=sampleLimit) df=dfsample.limit(sampleLimit)
    }

    df
  }

  def fillNullValues(df: DataFrame, value: String)= {
    df.na.fill(value)
  }

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
    // default to from is 0 (to merge easily with UI, also can extend feature to support *to*)
    val cols = df.columns.slice(from, to)
    df.select(cols.head, cols.tail: _*)
  }

  def getColumns(df : DataFrame) : String = {
    df.columns.mkString(",")
  }

  def getRowColumnMap (df : DataFrame) : String ={
    val cols = df.columns
    val rowMappedToColumns = df.map( f => f.getValuesMap(cols)).map(_.mkString(",").replace(" -> "," "))
    rowMappedToColumns.collect().mkString("({" , "} {", "})")
  }

  //Not used in Service : Usage agg functions to be in a string map grpcol as a list val result = groupAndAggregate(df, Map("COMUNE"-> "COUNT"), listOfStrings)
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
      if(colAndFun.matches(".*:MERGE")){
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
      dfUpdated = dfUpdated.withColumn(colName, dfUpdated.col(colName).cast(ScalableGrafterInterOpHelper.getFieldTypeInSchema(splits(1))))
      if(splits(0)=="desc") listOfColumn += col(colName).desc
      else if(splits(0)=="asc") listOfColumn += col(colName).asc
    }
    dfUpdated.sort(listOfColumn.toList: _*)
  }



  def addColumnWithFunctions(df : DataFrame , colName: String, funcStr: String): DataFrame ={
    df.withColumn(colName, UtilityFunctions.getUtilityUDF(funcStr))
  }

  def addColumnWithValue(df: DataFrame , colName: String,  value: Any) : DataFrame = {
    df.withColumn(colName, lit(value))
  }

  def dropColumn(df: DataFrame, from: Int, to: Int) : DataFrame ={
    val selected = df.columns.filterNot(df.columns.slice(from,to).contains(_))
    df.select(selected.head , selected.tail: _*)
  }

  def dropColumn(df: DataFrame, dropList: List[String]) : DataFrame = {
    val selected = df.columns.filterNot(dropList.contains(_))
    df.select(selected.head , selected.tail: _*)
  }

  def applyToColumn(df : DataFrame, colName: String, funcStr: List[String]) : DataFrame = {
    df.withColumn(colName, UtilityFunctions.getApplyFunctionForColumn(colName, funcStr))
  }
  def deriveColumn(df : DataFrame , newColName: String, deriveFrom: List[String], funcStr: List[String]) :DataFrame = {
    df.withColumn(newColName, UtilityFunctions.getApplyFunctionForColumn(deriveFrom.head, funcStr))
  }

  def splitColumn(df: DataFrame, colName: String, separator: String): DataFrame = {

    val colVal = df.select(col(colName)).head().getAs[String](0).split(separator)
    val list = scala.collection.mutable.ListBuffer[StructField]()
    for ( index <- 1 to colVal.length ) {list += StructField(colName+"_splited_"+index, StringType,true)}

    val split_row= (colToSplit: String) => {colToSplit.split(separator)}

    val rows = df.rdd.map(r => Row.fromSeq(
      r.toSeq ++
        split_row(r.getAs[String](colName))))

    sqlContext.createDataFrame(rows, StructType(df.schema.fields ++ list))
  }

  def renameColumn(df: DataFrame, existingName: String, newName: String): DataFrame = {
    df.withColumnRenamed(existingName, newName)
  }

  def renameAllColumns(df: DataFrame,  newName: List[String]): DataFrame = {
    df.toDF(newName: _*)
  }


  def mergeColumn(df: DataFrame, newColName: String, colsToMerge : List[String], separator: String): DataFrame = {
    val listOfColumn = scala.collection.mutable.ListBuffer[Column]()
    for(colName <- colsToMerge) listOfColumn += col(colName)
    df.withColumn(newColName , concat_ws(separator , listOfColumn.toList: _* ))
  }

  def show(df: DataFrame): DataFrame = {
    df.show()
    df
  }

  def addRow(df: DataFrame, rowValues: List[String]): DataFrame = {
    val rowRdd = sqlContext.sparkContext.parallelize(Seq(rowValues)).map(v => Row(v: _*))
    val newDF = sqlContext.createDataFrame(rowRdd, df.schema)
    df.unionAll(newDF)
  }

/*  def takeRow(df: DataFrame, from: Int, to: Int) : DataFrame = {
    val subset = df.take(to).slice(from, to)
//    sqlContext.createDataFrame(subset, df.schema)
    df
  }*/

  def filterRows(df: DataFrame, colsToFilter: List[String], funcStr: String, expToFilter : String) : DataFrame = {
    df.filter(UtilityFunctions.getFilterExp(colsToFilter.head, funcStr, expToFilter))
  }

//  def melt(df: DataFrame) : Unit = {
//
//  }

  def saveDataAsJson(df: DataFrame, filePath: String): String = {
    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val hdfs = FileSystem.get(hadoopConf)
    val path = new Path(filePath)
    if (hdfs.exists(path)) {
      hdfs.delete(path, true)
    }
    val mergedPath = "merged-"+filePath+".json"
    val merged = new Path (mergedPath)
    if (hdfs.exists(merged)) {
      hdfs.delete(merged, true)
    }
    df.toJSON.saveAsTextFile(filePath)

    FileUtil.copyMerge(hdfs, path, hdfs, merged, false, hadoopConf, null)
    mergedPath
  }
  /**
    * Creates a csv file of given DataFrame. Overwrites if anything already exist in provided output path
    *
    * @todo Make it without repartition and merge them to new file and return the new file location as output
    * @todo analyse for any better solutions in future
    * @param df DataFrame to save
    * @param filePath Physical location to save dataframe
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
      .option("header", "false")
      .mode(SaveMode.Overwrite)
      .save(filePath)

    FileUtil.copyMerge(hdfs, path, hdfs, merged, false, hadoopConf, null)
//        hdfs.delete(path, true)
    mergedPath
  }



  def saveSampleAsCsv(df: DataFrame, filePath: String): String = {
    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val hdfs = FileSystem.get(hadoopConf)
    val path = new Path(filePath)

    val mergedPath = "merged-sample"+filePath+".csv"
    val merged = new Path (mergedPath)
    if (hdfs.exists(merged)) {
      hdfs.delete(merged, true)
    }
    df.repartition(1).write //assuming the sample is quite small and can fit in single memory
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(filePath)

    FileUtil.copyMerge(hdfs, path, hdfs, merged, false, hadoopConf, null)
//    hdfs.delete(path, true)
    mergedPath
  }
}

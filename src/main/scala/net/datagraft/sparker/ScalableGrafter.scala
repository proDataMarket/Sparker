package net.datagraft.sparker

import net.datagraft.sparker.core.InitSpark
import net.datagraft.sparker.rdf.RDFTransformer
import net.datagraft.sparker.tabular.TabularTransformer


/**
  * Created by nive on 4/5/2016.
  */
object ScalableGrafter {

  def main(args: Array[String]) {

    val scalableSpark = new InitSpark().init()
    val transformer = new TabularTransformer(scalableSpark)
    var df = transformer.makeDataSet(args(0))
//    df = transformer.dropRows(df,0,3)
    df = transformer.makeDataSetWithColumn(df)
    df = transformer.filterRows(df,List("Product"), "match", List( "take", "Debt collection"))
    df = transformer.removeDuplicates(df, List("State"))
    df = transformer.groupAndAggregate(df, List("Sub-issue"), List("Product:COUNT"))
    df = transformer.deriveColumn(df, "person-uri", List("Sub-issue"), List("prefixer", "http://person.com/") )

//    val rdftransform = new RDFTransformer();

//    val rdd = rdftransform.createNode(df, "name");
//    df.show()
//    df = transformer.melt(df, List("name"))
//    df = transformer.makeDataSet(df, List("county", "industry", "gender", "age", "2008", "2009", "2010", "2011", "2012", "2013", "2014"))
//    df = transformer.applyToColumn(df, "county", List("fill-empty-with", "test"))
//    df = transformer.fillWhen(df, "county")

//    transformer.saveSampleAsCsv(df, "ConsumerComplaits")

//    transformer.saveDataAsCsv(df, "sample2")

//    println(df.collect().length)
//    df = transformer.makeDataSetWithColumn(df)
//    df = transformer.filterRows(df, List("COMUNE"), "regex" ,List("drop", ".*airasca.*") )
//    df = transformer.addColumnWithFunctions(df,"rowids", "Row number")

//    df = transformer.takeRows(df, 0,50)


//    df = transformer.groupAndAggregate(df, List("CODREG" ,"REGIONE" ,"CODPRO", "PROVINCIA" ,"CODCOM", "COMUNE"), List("CODLOC:MERGE"))
//    df = transformer.applyToColumn(df, "Date sent to company" , List("date" , "MM/dd/yyyy"))
//    df = transformer.sortDataSetWithColumnExpr(df, List("Date sent to company"), List("desc:date"))

//    val tet = //= ("nive", "f", "90", "world", "23456")
//    Traversable("nive", "f", "90", "world", "23456")
//    df = transformer.addRow(df,List("ann", "f", "90", "world", "23456") )
//    df = transformer.filterRows(df, List("age"),"+", "63")
//      df = transformer.deriveColumn(df, "newone", List("age") , List("prefixer", "http://example.com/persons#"))
//      df = df.withColumn("new" ,lit(df.groupBy().max("age").collect().head))
//    df = transformer.deriveColumn(df, "newOne", List("age"), "max")

//    df = transformer.dropColumn(df, List("age","street"))

//    df = transformer.renameColumn(df, "newCol_splited_2", "name")
//    df = transformer.dropColumn(df , List("name"))
//    df = transformer.deriveColumn(df, "newCol" , List("age"), "max")

//    df = transformer.mergeColumn(df, "newCol" , List("age", "name", "street"), ":")
//        df = transformer.splitColumn(df, "newCol" , ":")
      df.show()
//    transformer.saveDataAsCsv(df, "newMergedFile")

//    val cols = List("name", "sex")
//    val aggcol = List("street:agg" , "age:agg" , "street:first")
////    val aggfun = List("agg" , "agg")
//    df = transformer.groupAndAggregate(df, cols, aggcol)
//    df.show()


//    println(transformer.getColumns(df))
//    val test = transformer.getRowColumnMap(df)
//
//    println(test)

//    df = transformer.paginateDataFrame(df, 3,45)

//    df = transformer.removeDuplicates(df)
//    transformer.saveDataAsCsv(df, "results.csv")
//    df.take(100)
//
//    val listOfCols =  List("CODREG", "REGIONE", "CODPRO","PROVINCIA", "CODCOM", "COMUNE" );
////
//    df = transformer.makeDataSet(df, 15)
////    //    df = makeDataSet(df, 6)
////
//    val listOfStrings =  List("CODREG", "REGIONE", "CODPRO", "PROVINCIA", "CODCOM");
//    val col = List("age","weight")
//    val func = List( "desc: int" ,  "desc: num")
//    df = transformer.removeDuplicates(df,listOfStrings )

//
//    transformer.saveDataAsCsv(df, "results.csv")
//    df =transformer.sortDataSetWithColumnExpr(df, col, func)
//    df.printSchema()
//    df.show()
    //    df = sortDataSet(df, Map("CODCOM"-> "NUM:DESC", "first(COMUNE)"-> "ALPHA:ASC"))
//    println(transformer.saveDataAsJson(df, "resu"))
//    df.show
    //    result.show









//    val conf = new SparkConf().setAppName("Sparkify").setMaster("local[*]")
//    val sparkContext =new SparkContext(conf)
//    val sqlContext = new SQLContext(sparkContext)
//    var df =  sqlContext.read
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .load("example-data.csv")
////    val colsToSort= List("age")
//    df.withColumn("age", $"age".cast(IntegerType)).sort($"age".desc)
//    df.printSchema()
//    df.show()
//    sparkContext.stop()

  }

}

package net.datagraft.sparker

import net.datagraft.sparker.core.{Transformations, InitSpark}


/**
  * Development Tester class
  * Created by nive on 4/5/2016.
  */
object ScalableGrafter {

  def main(args: Array[String]) {

    val scalableSpark = new InitSpark().init()
    val transformer = new Transformations(scalableSpark)
    var df = transformer.makeDataSet("example-data.csv")

    df =transformer.makeDataSetWithColumn(df)
    df = transformer.makeDataSet(df, 15)
    df.show()

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
    val col = List("age","weight")
    val func = List( "desc: int" ,  "desc: num")
//    df = transformer.removeDuplicates(df,listOfStrings )

//
//    transformer.saveDataAsCsv(df, "results.csv")
    df =transformer.sortDataSetWithColumnExpr(df, col, func)
//    df.printSchema()
    df.show()
    //    df = sortDataSet(df, Map("CODCOM"-> "NUM:DESC", "first(COMUNE)"-> "ALPHA:ASC"))
//    println(transformer.saveDataAsCsv(df, "resu"))
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

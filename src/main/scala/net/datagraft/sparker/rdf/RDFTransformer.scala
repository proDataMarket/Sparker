package net.datagraft.sparker.rdf

import  org.apache.spark.graphx._
import org.apache.hadoop.fs.{FileUtil, Path, FileSystem}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode, DataFrame}

import scala.util.hashing.MurmurHash3

/**
  * Created by nive on 5/2/2016.
  */
class RDFTransformer(sparkCont: SparkContext) {


  val sqlContext = new SQLContext(sparkCont)
  def createVertices(df: DataFrame, nodeName: String) : Unit ={
//    df.select(nodeName).map(r => r(0).toString)

    val baseURI = "http://person.com/xpropgraph#"
    val flightsFromTo = df.select("name", "sex")// base uri and mapping
    val airportCodes = df.select("name", "sex").flatMap(x => Iterable(x(0).toString, x(1).toString))
    //    val airportCodes = df.select($"Origin", $"Dest").flatMap(x => Iterable(x(0).toString, x(1).toString))
    //    transformer.makeDataSet(args(0), true, 3000)

    val airportVertices: RDD[(VertexId, String)] = airportCodes.distinct().map(x => (MurmurHash3.stringHash(x).toLong,x))
    val defaultAirport = "Missing"

    val flightEdges = flightsFromTo.map(x =>
      (MurmurHash3.stringHash(x(0).toString),MurmurHash3.stringHash(x(1).toString))).map(x => Edge(x._1, x._2, s"<$baseURI${"mappinghere"}>"))
    val graph = Graph(airportVertices, flightEdges, defaultAirport)

    saveTripletsAsNT(graph.triplets.map(triplet =>  s"<$baseURI${triplet.srcAttr}> ${triplet.attr} <$baseURI${triplet.dstAttr}> ."), "RDFTest")
  }
//
//  def createEdges(data: DataFrame, baseNode: String, endNode: String, mappring: String) : RDD[String] = {
//    data.select(baseNode, endNode).map(r => r(0).toString+ r(1).toString+ mappring)
//  }
//
//  def createGraph(vertices: RDD, edges: RDD[Edge], defaultNode: String): Graph = {
//    Graph(vertices, edges, defaultNode)
//  }
//  def createEdge(data: DataFrame, subject: String, property: String, obj: String) : Edge

  def saveTripletsAsNT(graph: RDD[String], filePath: String) : Unit ={
    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val hdfs = FileSystem.get(hadoopConf)
    val path = new Path(filePath)
    if (hdfs.exists(path)) {
      hdfs.delete(path, true)
    }
    val mergedPath = "Merged" + filePath + ".nt"
    val merged = new Path(mergedPath)
    if (hdfs.exists(merged)) {
      hdfs.delete(merged, true)
    }
    graph.saveAsTextFile(filePath)

    FileUtil.copyMerge(hdfs, path, hdfs, merged, false, hadoopConf, null)
    mergedPath
  }
}

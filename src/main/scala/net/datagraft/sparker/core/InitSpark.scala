package net.datagraft.sparker.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Configure spark context for application
  * Created by nive on 4/5/2016.
  */
class InitSpark {

  def init(): SparkContext ={
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local[*]")
    new SparkContext(conf)
  }
}

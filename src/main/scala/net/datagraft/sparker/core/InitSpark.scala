package net.datagraft.sparker.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Configure spark context for application
  * Created by nive on 4/5/2016.
  */
class InitSpark {

  def init(): SparkContext ={

    val sparkConf = new SparkConf();
    sparkConf.set("spark.app.name", "Sparker")
//    sparkConf.set("spark.master", "yarn")
    //    sparkConf.set("spark.deploy.mode", "client")


    new SparkContext(sparkConf)
  }
}

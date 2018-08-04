package com.navecom.logs.driver

import com.navecom.logs.config.ProjectConfig
import com.navecom.logs.spark.SparkContextFactory
import com.navecom.logs.utils.{AccessLog, LogUtils}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.FileUtils

object logAnalytics extends ProjectConfig{
  def main(args : Array[String]): Unit ={

    /*
      Getting the sparkContext from ConfigFactory
     */
    val sparkContext:SparkContext=SparkContextFactory.getSparkContext("local")

    /*
      Reading the log data file
    */
    val accessLogs : RDD[AccessLog] = FileUtils.readAsText(logDataFile,sparkContext).map(AccessLog.parseLogLine)

    /*
       getting how many times each url was called
   */
    val uRLsCount : RDD[(String,Int)] = LogUtils.eachURLCalls(accessLogs)

  /*
      Get all the departments
  */
  val department :RDD[String]= LogUtils.departments(accessLogs)


  }
}

package com.navecom.logs.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

class SparkContextFactory {

  def prepareSparkConfig(environment : String) : SparkConf = {
    var config : SparkConf =null;
    if (environment != null && (environment.equalsIgnoreCase("local")
      || environment.equalsIgnoreCase("standalone"))) {

      config = new SparkConf().setMaster("local[*]").setAppName("Credit Card Analytics")

    } else if (environment != null && (environment.equalsIgnoreCase("yarn")
      || environment.equalsIgnoreCase("cluster"))) {
      /* TODO */
    }
    config
  }

  def getSparkContext(environment: String) : SparkContext={
    val sparkConfig=prepareSparkConfig(environment);
    var sparkContext:SparkContext=null;

    if (sparkConfig != null) {
      sparkContext = new SparkContext(sparkConfig)
    }
    sparkContext
  }

  def getSparkSession(environment:String):SparkSession ={
    val sparkConfig=prepareSparkConfig(environment);

    val sparkSession=SparkSession.builder().master("local[*]").getOrCreate();
    setProperties(sparkSession)
    /* For implicit conversions like converting RDDs to DataFrames */
    import sparkSession.implicits._
    sparkSession
  }

  /**
    *
    * Set all Application properties for spark session
    */
  def setProperties(sparkSession: SparkSession) = {

    /* Print the spark version */
    println("=**=Spark version: " + sparkSession.sparkContext.version + "=**=")

    /* Change log level to avoid lots of log */
    sparkSession.sparkContext.setLogLevel("ERROR")

    sparkSession.conf.set("spark.app.name", "Spark Log Analytics")

    /* Use as many threads as cores */
    sparkSession.conf.set("spark.master", "local")

    /* Default 1, used in cluster */
    sparkSession.conf.set("spark.driver.cores", "1")

    /* Default 1g */
    sparkSession.conf.set("spark.driver.maxResultSize", "1g")
    sparkSession.conf.set("spark.driver.memory", "1g")
    sparkSession.conf.set("spark.executor.memory", "1g")

    sparkSession.conf.set("spark.extraListeners", "com.navecom.logs.spark.MySparkListener")

    /* Default is 'temp' */
    sparkSession.conf.set("spark.local.dir", "staging")

    /* Default is false */
    sparkSession.conf.set("spark.logConf", "true")

    /* Default None, client for local, cluster for cluster environment */
    sparkSession.conf.set("spark.submit.deployMode", "client")

    /* Port on which the external shuffle service will run. Default is 7337 */
    sparkSession.conf.set("spark.shuffle.service.port", "7337")

    /* Base directory in which Spark events are logged, if spark.eventLog.enabled is true */
    /* Within this base directory, Spark creates a sub-directory for each application, and logs the events specific to the application in this directory */
    /* Users may want to set this to a unified location like an HDFS directory so history files can be read by the history server */
    sparkSession.conf.set("spark.eventLog.dir", "event-log-dir") //Default

    /* Whether to log Spark events, useful for reconstructing the Web UI after the application has finished. Default is false */
    sparkSession.conf.set("spark.eventLog.enabled", "true")

    /* Spark UI port. Default is 4040 */
    sparkSession.conf.set("spark.ui.port", "4040")
  }


}


/* Companion object */
object SparkContextFactory extends com.navecom.logs.config.ProjectConfig {

  /* To avoid 'winutils.exe' error on windows */
  /*  if (System.getProperty("os.name").toLowerCase.contains("window")) {
      System.setProperty("hadoop.home.dir", winUtils);
    }*/

  /* Create singleton instance of class */
  private val factory: SparkContextFactory = new SparkContextFactory

  def getSparkSession(enviornment: String): SparkSession = {
    /* Get spark session*/
    factory.getSparkSession(enviornment)
  }
  /* Get spark context
   */
  def getSparkContext(environment: String):SparkContext ={
    factory.getSparkContext(environment)
  }
}

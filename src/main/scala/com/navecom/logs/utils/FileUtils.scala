package util

import com.navecom.logs.config.ProjectConfig
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


trait CustomFileReader {
  def readAsCSV(filename:String,sparkSession: SparkSession):DataFrame
  def readAsText(filename:String,sparkContext: SparkContext):RDD[String]
}
trait CustomFileWriter{
  def writeAsText(df: DataFrame, location: String, writeSingle: Boolean)
  def writeAsCSV(df: DataFrame, location: String, writeSingle: Boolean)
}

object FileUtils extends CustomFileReader with CustomFileWriter with ProjectConfig{

  /* Read operation(s) */
  override def readAsCSV(filename: String, sparkSession: SparkSession): DataFrame =
    sparkSession.read.option("header", "true").
      option("mode", "DROPMALFORMED").
      format("com.databricks.spark.csv").
      csv(filename)

  override def readAsText(filename: String, sparkContext: SparkContext): RDD[String] ={
    sparkContext.textFile(filename)
  }

  /* Write operation(s) */
  override def writeAsText(df: DataFrame, location: String, writeSingle: Boolean) = {
    if (writeSingle) { df.coalesce(1).rdd.saveAsTextFile("users") }
  }

  override def writeAsCSV(df: DataFrame, location: String, writeSingle: Boolean) = {
    if (writeSingle) {
      df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(location)
    }
  }

}

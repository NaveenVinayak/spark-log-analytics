package com.navecom.logs.config

import com.typesafe.config.ConfigFactory

trait ProjectConfig {
  private val config=ConfigFactory.load()

  val data=config.getConfig("data")
// Aythala
  val logDataFile=data.getString("logDataFile.file")
//Trying to commit
// Adding to check if it comess
}

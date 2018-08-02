package com.navecom.logs.config

import com.typesafe.config.ConfigFactory

trait ProjectConfig {
  private val config=ConfigFactory.load()

  val data=config.getConfig("data")

  val logDataFile=data.getString("logDataFile.file")
//Trying to commit
}

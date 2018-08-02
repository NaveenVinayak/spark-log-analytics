package com.navecom.logs.utils

import com.navecom.logs.config.ProjectConfig
import org.apache.spark.rdd.RDD

object LogUtils extends ProjectConfig {

  /*
  This function takes accessLogs as input and
  returns how many times each url was called
 */
  def eachURLCalls(accessLogs: RDD[AccessLog])={
    val eachURLCalls =accessLogs.map( x =>x.endpoint).
      map(x => (x,1)).
      reduceByKey(_+_)
  }

  /*
  This function takes accessLogs as input and
  returns unique departments
  */

  def departments(accessLogs: RDD[AccessLog]) ={
    val departments = accessLogs.map(x=>x.endpoint).map(endpt => {
      val x = endpt.split("/")
      x(1)
    }).distinct()
  }

  /*
  This function takes accessLogs as input and
  returns how many times each department was called
  */

  def departmentsCount(accessLogs: RDD[AccessLog]) ={
    val departments = accessLogs.map(x=>x.endpoint).map(endpt => {
      val x = endpt.split("/")
      (x(1),1)
    }).reduceByKey(_+_)
  }

  // user who visited sports department more than 10 times
  def departmentsCount1(accessLogs: RDD[AccessLog]) = {
    val sportdept = accessLogs.map(x => (x.clientIdentd, x.endpoint)).//filter(x => x._2.split("/")(1)=="sport")
      filter(x => {
      val endpoint = x._2
      val d = endpoint.split("/")
      (d(1) == "sport")
    }).map(x => (x, 1)).reduceByKey(_+_).filter(x => x._2.toInt>10)

  }

//total-login from each user
  def LoginCountFromEachUser(accessLogs: RDD[AccessLog]) = {
    val requestIniated =accessLogs.map(x => (x.clientIdentd,x.endpoint)).
      filter(x => x._2.split("/")(0)=="log-in").
      map(x => (x,1)).
      reduceByKey(_+_).
      map(x => (x._1._1,x._2))
  }

  //how many add to cart from each user
  def addToCart(accessLogs: RDD[AccessLog]) = {
    val requestIniated =accessLogs.map(x => (x.clientIdentd,x.endpoint)).
      filter(x => x._2.split("/")(0)=="add-to-cat").
      map(x => (x,1)).
      reduceByKey(_+_).
      map(x => (x._1._1,x._2))
  }

  //calls from mac
  def callFromMac(accessLogs: RDD[AccessLog]) = {
    val requestIniated =accessLogs.map(x => x.browser).
      filter(x => x.split(" ")(0)=="(Macintosh").
      map(x => (x,1)).
      reduceByKey(_+_).
      map(x => ("MacCalls",x._2))
  }

  def callFromWindows(accessLogs: RDD[AccessLog]) = {
    val requestIniated =accessLogs.map(x => x.browser).
      filter(x => x.split(" ")(0)=="(Windows").
      map(x => (x,1)).
      reduceByKey(_+_).
      map(x => ("MacCalls",x._2))
  }

  def callFromLinux(accessLogs: RDD[AccessLog]) = {
    val requestIniated =accessLogs.map(x => x.browser).
      filter(x => x.split(" ")(0)=="(Linux").
      map(x => (x,1)).
      reduceByKey(_+_).
      map(x => ("MacCalls",x._2))
  }

  // what are the responsecode and how many ttimes they were returned

  def reasonCodes(accessLogs: RDD[AccessLog]) = {
    val requestIniated =accessLogs.map(x => (x.responseCode,1)).
      reduceByKey(_+_)
  }

  // how much bytes were transferred

  def bytestransfreed(accessLogs: RDD[AccessLog]) = {
    val requestIniated =accessLogs.map(x => x.contentSize).
      reduce(_+_)
  }

  // 404 url
  def url404(accessLogs: RDD[AccessLog]) = {
    val requestIniated =accessLogs.map(x => (x.endpoint,x.responseCode)).filter(x => x._2.toInt==404).map(x=>x._1)
  }


  //how many check-out from each user
  def checkedout(accessLogs: RDD[AccessLog]) = {
    val requestIniated =accessLogs.map(x => (x.clientIdentd,x.endpoint)).
      filter(x => x._2.split("/")(0)=="check-out").
      map(x => (x,1)).
      reduceByKey(_+_).
      map(x => (x._1._1,x._2))
  }

  //checked out everyday
  def checkedoute(accessLogs: RDD[AccessLog]) = {
    val requestIniated =accessLogs.map(x => (x.clientIdentd,x.endpoint)).
      filter(x => x._2.split("/")(0)=="check-out").
      map(x => (x,1)).
      reduceByKey(_+_).
      map(x => (x._1._1,x._2))
  }

  // user who visited sports department more than 10 times
  def golf(accessLogs: RDD[AccessLog]) = {
    val sportdept = accessLogs.map(x => (x.clientIdentd, x.endpoint)).//filter(x => x._2.split("/")(1)=="sport")
      filter(x => {
      val endpoint = x._2
      val d = endpoint.split("/")
      (d(1) == "golf")
    }).map(x => x._1)
  }
  // user who visited sports department more than 10 times
  def fitness(accessLogs: RDD[AccessLog]) = {
    val sportdept = accessLogs.map(x => (x.clientIdentd, x.endpoint)).//filter(x => x._2.split("/")(1)=="sport")
      filter(x => {
      val endpoint = x._2
      val d = endpoint.split("/")
      (d(1) == "fitness")
    }).map(x => x._1)
  }


}

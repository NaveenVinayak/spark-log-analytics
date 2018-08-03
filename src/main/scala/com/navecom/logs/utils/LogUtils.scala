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

  /*
  This function takes accessLogs and  number 'n'
  and returns the users who all visited sports department more than n times
  */

  def sportsUserVisits(accessLogs: RDD[AccessLog],n:Int) = {
    val sportdept = accessLogs.map(x => (x.clientIdentd, x.endpoint)).//filter(x => x._2.split("/")(1)=="sport")
      filter(x => {
      val endpoint = x._2
      val d = endpoint.split("/")
      d(1) == "sport"
    }).map(x => (x, 1)).reduceByKey(_+_).filter(x => x._2.toInt>n)

  }

  /*
  This function takes accessLogs as input and
  returns total logins of each user
  */
  def LoginCountFromEachUser(accessLogs: RDD[AccessLog]) = {
    val requestIniated =accessLogs.map(x => (x.clientIdentd,x.endpoint)).
      filter(x => x._2.split("/")(0)=="log-in").
      map(x => (x,1)).
      reduceByKey(_+_).
      map(x => (x._1._1,x._2))
  }


  /*
  This function takes accessLogs as input and
  returns total add-to-cart from each user
  */
  def addToCart(accessLogs: RDD[AccessLog]) = {
    val requestIniated =accessLogs.map(x => (x.clientIdentd,x.endpoint)).
      filter(x => x._2.split("/")(0)=="add-to-cat").
      map(x => (x,1)).
      reduceByKey(_+_).
      map(x => (x._1._1,x._2))
  }

  /*
  This function takes accessLogs as input and
  returns total calls from the Mac book
  */
  def callFromMac(accessLogs: RDD[AccessLog]) = {
    val requestIniated =accessLogs.map(x => x.browser).
      filter(x => x.split(" ")(0)=="(Macintosh").
      map(x => (x,1)).
      reduceByKey(_+_).
      map(x => ("MacCalls",x._2))
  }

  /*
  This function takes accessLogs as input and
  returns total calls from the windows
  */

  def callFromWindows(accessLogs: RDD[AccessLog]) = {
    val requestIniated =accessLogs.map(x => x.browser).
      filter(x => x.split(" ")(0)=="(Windows").
      map(x => (x,1)).
      reduceByKey(_+_).
      map(x => ("MacCalls",x._2))
  }

  /*
  This function takes accessLogs as input and
  returns total calls from the Linux OS
  */
  def callFromLinux(accessLogs: RDD[AccessLog]) = {
    val requestIniated =accessLogs.map(x => x.browser).
      filter(x => x.split(" ")(0)=="(Linux").
      map(x => (x,1)).
      reduceByKey(_+_).
      map(x => ("MacCalls",x._2))
  }

  /*
  This function takes accessLogs as input and
  returns responseCodes and thier counts
  */
  def responseCodeCounts(accessLogs: RDD[AccessLog]) = {
    val requestIniated =accessLogs.map(x => (x.responseCode,1)).
      reduceByKey(_+_)
  }

  /*
  This function takes accessLogs as input and
  returns total bytes transferred from the website
  */
  def bytestransfreed(accessLogs: RDD[AccessLog]) = {
    val requestIniated =accessLogs.map(x => x.contentSize).
      reduce(_+_)
  }

  /*
  This function takes accessLogs as input and
  returns all the URLs showed 404 */
  def url404(accessLogs: RDD[AccessLog]) = {
    val requestIniated =accessLogs.map(x => (x.endpoint,x.responseCode)).filter(x => x._2.toInt==404).map(x=>x._1)
  }

  /*
  This function takes accessLogs as input and
  returns total number of checkouts from each user
  */
  def checkedout(accessLogs: RDD[AccessLog]) = {
    val requestIniated =accessLogs.map(x => (x.clientIdentd,x.endpoint)).
      filter(x => x._2.split("/")(0)=="check-out").
      map(x => (x,1)).
      reduceByKey(_+_).
      map(x => (x._1._1,x._2))
  }

  //checked out everyday - To Do
  def checkedoute(accessLogs: RDD[AccessLog]) = {
    val requestIniated =accessLogs.map(x => (x.clientIdentd,x.endpoint)).
      filter(x => x._2.split("/")(0)=="check-out").
      map(x => (x,1)).
      reduceByKey(_+_).
      map(x => (x._1._1,x._2))
  }

  /*
  This function takes accessLogs
  and returns the users who all visited golf department
  */

  def golf(accessLogs: RDD[AccessLog],n:Int) = {
    val sportdept = accessLogs.map(x => (x.clientIdentd, x.endpoint)).//filter(x => x._2.split("/")(1)=="sport")
      filter(x => {
      val endpoint = x._2
      val d = endpoint.split("/")
      d(1) == "golf"
    }).map(x => x._1)
  }


  /*
  This function takes accessLogs
  and returns the users who all visited fitness department
  */
  def fitness(accessLogs: RDD[AccessLog],n:Int) = {
    val sportdept = accessLogs.map(x => (x.clientIdentd, x.endpoint)).//filter(x => x._2.split("/")(1)=="sport")
      filter(x => {
      val endpoint = x._2
      val d = endpoint.split("/")
      d(1) == "fitness"
    }).map(x => x._1)
  }


}

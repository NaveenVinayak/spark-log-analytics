package com.navecom.logs.utils


import scala.util.matching.Regex


/* An entry of access log */

case class AccessLog(ipAddress : String,
                     clientIdentd : String,
                     userId: String,
                     dateTime: String,
                     method: String,
                     endpoint: String,
                     protocol: String,
                     responseCode: Int,
                     contentSize: Long,
                     detail:String,
                     browser:String
                    )


object AccessLog {

  /*
   115.183.1.178 - - [01/Aug/2014:11:51:22 -0400] "GET /department/footwear/categories HTTP/1.1" 200 958 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:30.0) Gecko/20100101 Firefox/30.0"
    */

  val PATTERN : Regex="""^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+) "(\S+)" "(\S|\s)+"""".r

  /**
    * Parse log entry from a string.
    *
    * @param log A string, typically a line from a log file
    * @return An entry of Apache access log
    * @throws RuntimeException Unable to parse the string
    */

  def parseLogLine(log : String) : AccessLog = {
    log match{

      case PATTERN(ipAddress, clientIdentd, userId, dateTime, method, endpoint, protocol, responseCode, contentSize, detail, browser)
      => AccessLog(ipAddress, clientIdentd, userId, dateTime, method, endpoint, protocol, responseCode.toInt,
        contentSize.toLong, detail, browser)

      case _ => throw new RuntimeException(s"""Cannot parse the log line : $log""")

    }
  }

}


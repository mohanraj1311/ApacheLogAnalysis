import org.apache.log4j._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat


case class resultRecord(userId:String, noOfPages:Int, noOfSessions:Int,
                        longestSessionDuration: Long, shortestSessionDuration: Long)

object ApacheLogAnalysis {

  def parseLine(row: String): (String, Long) = {
    val words = row.split(" ")
    if(words.length>6 && words(6).split("/").length>3) {
      val userId = words(6).split("/")(3)
      val datetime = words(3)
      val epoch = DateTime.parse(datetime, DateTimeFormat.forPattern("dd/MMM/YYYY:HH:mm:ss")).getMillis() / 1000
      (userId, epoch)
    }
    else{
      //Invalid records shall return this
      ("_",0L)
    }
  }

  def getSessions(userId:String, epochList:Seq[Long]):resultRecord={
    val noOfPages = epochList.size
    var noOfSessions = 0
    var longestSessionDuration = 0L
    var shortestSessionDuration = 0L
    val sortedEpochs = epochList.sorted

    if (sortedEpochs.nonEmpty) {
      var sessionStart = sortedEpochs.head
      noOfSessions = 1

      var i = 1
      while (i < noOfPages) {
        shortestSessionDuration = Long.MaxValue
        val accessDiff = sortedEpochs(i) - sortedEpochs(i - 1)
        if (accessDiff > 600) {
          val sessionEnd = sortedEpochs(i - 1)
          val sessionDuration = sessionEnd - sessionStart
          shortestSessionDuration = Math.min(shortestSessionDuration, sessionDuration)
          longestSessionDuration = Math.max(longestSessionDuration, sessionDuration)
          noOfSessions += 1
          sessionStart = sortedEpochs(i)
        }
        i += 1
      }

      // update session durations for last page access
      val lastSessionDuration = sortedEpochs(noOfPages - 1) - sessionStart
      shortestSessionDuration = Math.min(shortestSessionDuration, lastSessionDuration)
      longestSessionDuration = Math.max(longestSessionDuration, lastSessionDuration)
    }
    resultRecord(userId, noOfPages, noOfSessions, (longestSessionDuration / 60).toInt, (shortestSessionDuration / 60).toInt)
  }




  def printResultRecords(noOfPages:Int,resultRec:resultRecord)={
    println(resultRec.userId+"     "+resultRec.noOfPages+"     "+resultRec.noOfSessions
      + "    "+resultRec.longestSessionDuration + "    "+resultRec.shortestSessionDuration)

  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("ApacheLogAnalysis")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(args(0))
    val parsedLines = lines.map(parseLine)
    val validRDDs = parsedLines.filter(userIdAndEpoch=>userIdAndEpoch._1!="_")
    val epochByUserId = validRDDs.groupByKey()

    val userSessionRdd = epochByUserId.map { case (userId, epochs) => getSessions(userId, epochs.toSeq) }
    val sortedUserSessionRdd = userSessionRdd.map(res => (res.noOfPages, res)).sortByKey(ascending = false)

    println("Total no. of unique users : "+sortedUserSessionRdd.count())
    println("Top users:")
    println("id          # pages # sess  longest shortest")
    sortedUserSessionRdd.collect().take(5).foreach{case (noOfPages:Int,
    resultRecords:resultRecord)=>printResultRecords(noOfPages, resultRecords)}


  }
}

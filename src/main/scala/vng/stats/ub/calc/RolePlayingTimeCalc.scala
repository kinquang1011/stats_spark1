package vng.stats.ub.calc

import vng.stats.ub.report.BaseReport
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.sql.report.MysqlGameReport
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import vng.stats.ub.common.KpiFormat
import vng.stats.ub.utils.DateTimeUtils
import scala.collection.mutable.ListBuffer
import vng.stats.ub.utils.Common
import vng.stats.ub.utils.Constants

object RolePlayingTimeCalc extends BaseReport {

    var activityPath = ""

    override def readExtraParams(): Unit = {

        activityPath = parameters(Constants.Parameters.ACTIVITY_PATH)
    }
    
    def excute(sqlContext: SQLContext): DataFrame = {

        val date = logDate
        val id = calcId
        var lstLogFiles = DataUtils.getListFiles(activityPath, logDate, timing);
        var logDF = sqlContext.read.parquet(lstLogFiles: _*)

        var activityDF = logDF.select("game_code", calcId, "sid", "rid", "log_date", "action")
        var activityRDD = activityDF.rdd.map { row =>

            var gameCode = row.getString(0)
            var id = row.getString(1)
            var sid = row.getString(2)
            var rid = row.getString(3)
            var logDate = row.getString(4)
            var logType = row.getString(5)
            var timestamp = DateTimeUtils.getTimestamp(logDate)
            var flag = if (logType == "login") 1 else 0

            (gameCode, id, sid, rid, timestamp, flag)
        }

        var rolePlayTimeRDD = activityRDD.groupBy({ record => (record._1, record._2, record._3, record._4) }).mapValues({ values =>

            var playtime: Long = 0L
            var timestamp: Long = 0L
            var flag: Int = 0
            var list = values.toList.sortBy(_._5).to[ListBuffer]
            var i = 1
            var listSize = list.size
            var loginTime = 0L
            var logoutTime = DateTimeUtils.getTimestamp(date + " 24:00:00")
            
            while (i < listSize) {
                /**
                 * if two login action occur, remove first, two logout action occur remove second
                 */
                if (list(i - 1)._6 == list(i)._6) {
                    if (list(i)._6 == 1) {
                        list.remove(i - 1)
                    } else {
                        list.remove(i)
                    }
                    i -= 1
                    listSize -= 1
                }
                i += 1
            }

            // process specific value
            if (list.size > 0 && list(0)._6 == 0) {
                list.insert(0, (list(0)._1, list(0)._2, list(0)._3, list(0)._4, DateTimeUtils.getTimestamp(date + " 00:00:00"), 1))
            }

            if (list.size > 0 && list(list.size - 1)._6 == 1) {
                list.append((list(0)._1, list(0)._2, list(0)._3, list(0)._4, DateTimeUtils.getTimestamp(date + " 24:00:00"), 0))
            }

            // calculating
            var playingtime = 0L
            listSize = list.size - 1
            for (j <- 0 to listSize by 2) {
                loginTime = list(j)._5
                logoutTime = list(j + 1)._5
                playingtime += (logoutTime - loginTime)
            }
            (playingtime * 1.0) / (60 * 1000)    // unit in minutes
        })
        
        import sqlContext.implicits._
        rolePlayTimeRDD.toDF().selectExpr("_1._1 as game_code", s"""'$date' as log_date""", s"""_1._2 as $id""", "_1._3 as sid", "_1._4 as rid", "_2 as time")
    }
    
    override def write(df: DataFrame): Unit = {

        df.coalesce(1).write.mode("overwrite").format("parquet").save(outputPath + "/" + logDate)
    }
}
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
import org.apache.spark.sql.functions.sum
import vng.stats.ub.common.Schemas
import org.apache.spark.sql.Row

object AccountPlayingTimeCalc extends BaseReport {

    var rolePlayingTimePath = ""
    var accountPlayingTimePath = ""

    override def readExtraParams(): Unit = {

        rolePlayingTimePath = parameters(Constants.Parameters.ROLE_PLAYING_TIME)
        accountPlayingTimePath = parameters(Constants.Parameters.ACCOUNT_PLAYING_TIME)
    }
    
    def excute(sqlContext: SQLContext): DataFrame = {

        import sqlContext.implicits._
        
        var prevPayingTimeFile = DataUtils.getFile(accountPlayingTimePath, logDate, timing);
        var lstActivityFiles = DataUtils.getListFiles(rolePlayingTimePath, logDate, timing);
        var logDF = sqlContext.read.parquet(lstActivityFiles: _*)
        
        var timeDF = logDF.groupBy("game_code", calcId).agg(sum("time")).rdd.map { row =>
            var gameCode = row.getString(0)
            var id = row.getString(1)
            var time = row.getDouble(2)
            
            var rank = ranking(time)
            
            (gameCode, id, time, rank)
        }.toDF.selectExpr("_1 as game_code", "_2 as id", "_3 as time", "_4 as rank")
        
        var prevDF = sqlContext.createDataFrame(sc.emptyRDD[Row], Schemas.AccountPlayingTime)
        if (!DataUtils.isEmpty(prevPayingTimeFile)) {
            prevDF = sqlContext.read.parquet(prevPayingTimeFile)
        }
        
        var tempRDD = prevDF.as('p).join(timeDF.as('t),
                prevDF("game_code") === timeDF("game_code") && prevDF("id") === timeDF("id"),
                "full_outer").select("t.game_code","p.id","t.id","p.rank", "t.rank", "t.time").rdd
        var trendRDD = tempRDD.map { row =>
            
            var gameCode = row.getString(0)
            var prevId = row.getString(1)
            var curId = row.getString(2)
            var prevRank = row.getString(3)
            var curRank = row.getString(4)
            var time = row.getDouble(5)
            
            var id = curId
            if(null == curId){
                id = prevId
            }
            
            var trend = trending(prevRank, curRank)
            
            (gameCode, "timing", id, time, curRank, prevRank, trend)
        }.toDF
        
        trendRDD
    }
    
    override def write(df: DataFrame): Unit = {

        df.coalesce(1).write.mode("overwrite").format("parquet").save(outputPath + "/" + logDate)
    }
    
    
    def ranking(time: Double): String = {
        
        var rank: String = "A"
        if(time < 10){
            rank = "D"
        }else if(time < 100){
            rank = "C"
        }else if(time< 1000){
            rank = "B"
        }else{
            rank = "A"
        }
        
        rank
    }
    
    def trending(prevRank: String, curRank: String): String = {
        
        var cRank = curRank
        var pRank = prevRank
        var trend = "DECR"
        
        if(cRank == null){
            cRank = ""
        }
        
        if(pRank == null){
            pRank = ""
        }
        
            
        if(cRank.compareTo(pRank) == 0){
            trend = "STATBLE"
        }else if(cRank.compareTo(pRank) > 0){
            trend = "INCR"
        }
        
        trend
    }
}
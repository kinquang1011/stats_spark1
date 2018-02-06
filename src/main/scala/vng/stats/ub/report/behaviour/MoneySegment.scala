package vng.stats.ub.report.behaviour

import java.util.Calendar

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import vng.stats.ub.db.MysqlDB
import vng.stats.ub.utils.{Common, Constants, DateTimeUtils}

import scala.collection.immutable.HashMap

/**
 * Created by tuonglv on 07/10/2016.
 */
object MoneySegment {
/*
    var mapParameters: Map[String, String] = Map()
    val tableName = "custom_report"
    val reportCode = "money_segment"
    val mysqlDB = new MysqlDB()

    def main(args: Array[String]): Unit = {
        for (x <- args) {
            var xx = x.split("=")
            mapParameters += (xx(0) -> xx(1))
        }

        val conf = new SparkConf().setAppName("Money Segment Report::" + mapParameters("gameCode").toUpperCase)
        conf.set("spark.hadoop.validateOutputSpecs", "false")
        val sc = new SparkContext(conf)

        topRevenue(sc)
    }

    def getRange(limit1: Double, limit2: Double): String = {
        var l1 = ""
        var l2 = (limit2 / 1000000).toLong.toString
        if (limit1 == 0.0) {
            l1 = "0"
        } else {
            l1 = (limit1 / 1000000).toLong.toString
        }
        l1 + "-" + l2
    }

    def topRevenue(sc: SparkContext): Unit = {
        val sqlContext = new SQLContext(sc)
        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
        sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")

        var logDate = mapParameters("logDate")
        var source = mapParameters("source")
        var gameCode = mapParameters("gameCode")

        var moneyGroup = Array[Array[Double]]()
        moneyGroup = moneyGroup ++ Array(Array(0.0, 1000000.0))
        moneyGroup = moneyGroup ++ Array(Array(1000000.0, 2000000.0))
        moneyGroup = moneyGroup ++ Array(Array(2000000.0, 5000000.0))
        moneyGroup = moneyGroup ++ Array(Array(5000000.0, 10000000.0))
        moneyGroup = moneyGroup ++ Array(Array(10000000.0, 20000000.0))
        moneyGroup = moneyGroup ++ Array(Array(20000000.0, 50000000.0))
        moneyGroup = moneyGroup ++ Array(Array(50000000.0, 100000000.0))
        moneyGroup = moneyGroup ++ Array(Array(100000000.0, 300000000.0))
        moneyGroup = moneyGroup ++ Array(Array(300000000.0, 30000000000.0))

        var dataMap: Map[String, String] = HashMap()
        var deleteMap: Map[String, String] = HashMap()
        val now = Calendar.getInstance().getTime()
        val logDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val currentDateTime = logDateFormat.format(now)

        var characterInfoDf = sqlContext.read.parquet(Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.CHARACTER_INFO_OUTPUT_FOLDER, logDate, isSdk))
            .select("log_date", "id", "total_charge", "last_login_date", "last_charge_date")

        characterInfoDf.cache



        val count = moneyGroup.length
        for (x <- count - 1) {
            val limit1 = moneyGroup(x)(0)
            val limit2 = moneyGroup(x)(1)

            val data = characterInfoDf.filter("total_charge > " + limit1 + " and total_charge <= " + limit2)
            val moneyTotal = data.agg(sum("net_amt"))

        }
        characterInfoDf.unpersist()

        deleteMap += ("game_code" -> gameCode)
        deleteMap += ("timing" -> "0")
        deleteMap += ("log_date" -> logDate)
        deleteMap += ("source" -> source)
        deleteMap += ("report_code" -> reportCode)
        mysqlDB.deleteRecord(deleteMap, tableName)

        dataMap += ("game_code" -> gameCode)
        dataMap += ("log_date" -> logDate)
        dataMap += ("timing" -> "0")
        dataMap += ("calc_date" -> currentDateTime)
        dataMap += ("report_code" -> reportCode)


        //    mysqlDB.insertOrUpdate(deleteMap, dataMap, tableName)
        mysqlDB.insertRecord(dataMap, tableName)


        mysqlDB.close()
    }
    */
}
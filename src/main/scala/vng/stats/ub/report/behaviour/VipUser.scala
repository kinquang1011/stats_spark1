package vng.stats.ub.report.behaviour

import java.util.Calendar

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, SQLContext}
import vng.stats.ub.db.MysqlDB
import vng.stats.ub.utils.{DateTimeUtils, Constants, Common}
import org.apache.spark.sql.functions._

import scala.collection.immutable.HashMap

/**
 * Created by tuonglv on 07/10/2016.
 */
object VipUser {
    var mapParameters: Map[String, String] = Map()
    var tableName = "top_user"
    val mysqlDB = new MysqlDB()

    def main(args: Array[String]): Unit = {
        for (x <- args) {
            var xx = x.split("=")
            mapParameters += (xx(0) -> xx(1))
        }

        val conf = new SparkConf().setAppName("Top User Report::" + mapParameters("gameCode").toUpperCase)
        conf.set("spark.hadoop.validateOutputSpecs", "false")
        val sc = new SparkContext(conf)

        topRevenue(sc)
    }

    def topRevenue(sc: SparkContext): Unit = {
        val sqlContext = new SQLContext(sc)
        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
        sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")

        var limit: Int = 20
        if (mapParameters.contains("limit")) {
            limit = mapParameters("limit").toInt
        }
        var isSdk = false
        if (mapParameters.contains("isSdk") && mapParameters("isSdk") == "true") {
            isSdk = true
        }

        var logDate = mapParameters("logDate")
        var source = mapParameters("source")
        var gameCode = mapParameters("gameCode")

        var characterInfoDf = sqlContext.read.parquet(Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.CHARACTER_INFO_OUTPUT_FOLDER, logDate, isSdk))
        var paymentDf = sqlContext.read.parquet(Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.PAYMENT_OUTPUT_FOLDER, logDate, isSdk))
        //paymentTotalRevenueDf
        var pmtdf = paymentDf.groupBy("id", "sid", "rid").agg(sum("net_amt").as('total_charge))

        characterInfoDf.cache
        pmtdf.cache

        var selectedDay = logDate
        var last3Day = DateTimeUtils.getDateDifferent(-3, logDate, Constants.TIMING, Constants.A1)
        var last7Day = DateTimeUtils.getDateDifferent(-7, logDate, Constants.TIMING, Constants.A1)
        var last30Day = DateTimeUtils.getDateDifferent(-30, logDate, Constants.TIMING, Constants.A1)
        var timings:Map[String,String] = HashMap()
        timings += ("1" -> selectedDay)
        timings += ("3" -> last3Day)
        timings += ("7" -> last7Day)
        timings += ("30" -> last30Day)
        timings += ("all" -> "all")

        timings.keys.foreach { timing =>
            var topRecharge: DataFrame = null
            if (timing != "all") {
                topRecharge = characterInfoDf.filter("register_date >= '" + timings(timing) + "'").sort(desc("total_charge")).limit(limit)
            } else {
                topRecharge = characterInfoDf.sort(desc("total_charge")).limit(limit)
            }

            var join1 = topRecharge.as('t).join(pmtdf.as('b),
                topRecharge("id") === pmtdf("id") and topRecharge("rid") === pmtdf("rid") and topRecharge("sid") === pmtdf("sid"), "left_outer").selectExpr(
                    "t.*", "b.total_charge as today_charge")

            var dataMap: Map[String, String] = HashMap()
            var deleteMap: Map[String, String] = HashMap()

            val now = Calendar.getInstance().getTime()
            val logDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val currentDateTime = logDateFormat.format(now)

            deleteMap += ("game_code" -> gameCode)
            deleteMap += ("timing" -> timing)
            deleteMap += ("log_date" -> logDate)
            deleteMap += ("source" -> source)

            mysqlDB.deleteRecord(deleteMap, tableName)

            join1.collect().foreach { row =>
                dataMap = dataMap.empty
                dataMap += ("game_code" -> gameCode)
                dataMap += ("log_date" -> logDate)
                dataMap += ("timing" -> timing)
                dataMap += ("sid" -> row.getAs("sid"))
                dataMap += ("id" -> row.getAs("id"))
                dataMap += ("rid" -> row.getAs("rid"))
                dataMap += ("level" -> row.getAs("level").toString)
                dataMap += ("register_date" -> row.getAs("register_date"))
                dataMap += ("source" -> source)
                dataMap += ("calc_date" -> currentDateTime)

                if(row.isNullAt(row.fieldIndex("online_time"))){
                    dataMap += ("online_time" -> "0")
                }else{
                    dataMap += ("online_time" -> row.getAs("online_time").toString)
                }

                if(row.isNullAt(row.fieldIndex("last_login_date"))){
                    dataMap += ("last_login_date" -> "")
                }else{
                    dataMap += ("last_login_date" -> row.getAs("last_login_date").toString)
                }

                if(row.isNullAt(row.fieldIndex("first_charge_date"))){
                    dataMap += ("first_charge_date" -> "")
                }else{
                    dataMap += ("first_charge_date" -> row.getAs("first_charge_date").toString)
                }

                if(row.isNullAt(row.fieldIndex("last_charge_date"))){
                    dataMap += ("last_charge_date" -> "")
                }else{
                    dataMap += ("last_charge_date" -> row.getAs("last_charge_date").toString)
                }

                if(row.isNullAt(row.fieldIndex("total_charge"))){
                    dataMap += ("total_charge" -> "0")
                }else{
                    dataMap += ("total_charge" -> row.getAs("total_charge").toString)
                }

                if(row.isNullAt(row.fieldIndex("today_charge"))){
                    dataMap += ("today_charge" -> "0")
                }else{
                    dataMap += ("today_charge" -> row.getAs("today_charge").toString)
                }

                deleteMap += ("game_code" -> gameCode)
                deleteMap += ("timing" -> timing)
                deleteMap += ("log_date" -> logDate)
                deleteMap += ("sid" -> row.getAs("sid"))
                deleteMap += ("id" -> row.getAs("id"))
                deleteMap += ("rid" -> row.getAs("rid"))
                deleteMap += ("source" -> source)

            //    mysqlDB.insertOrUpdate(deleteMap, dataMap, tableName)
                mysqlDB.insertRecord(dataMap, tableName)
            }
        }
        characterInfoDf.unpersist()
        pmtdf.unpersist()

        mysqlDB.close()
    }
}
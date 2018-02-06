package vng.stats.ub.report.device

/**
 * Created by tuonglv on 07/07/2016.
 */


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import vng.stats.ub.config.DeviceConfig
import vng.stats.ub.db.MysqlDB
import vng.stats.ub.utils.{Common, Constants, DataUtils}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.immutable.HashMap
import scala.util.Try
import scala.util.control.Exception.allCatch
import java.util.Calendar

import scala.util.matching.Regex

object DeviceReport {

    val NO_DATA: String = "nodata"
    val HOME_DIR: String = "/ge/warehouse"

    //val oldGames: List[String] = List("contra", "dttk", "pmcl", "tlbbm", "tvl", "wefight")
    val oldGames: List[String] = List("aaaaaaa")

    def main(args: Array[String]): Unit = {
        var mapParameters: Map[String, String] = Map()
        for (x <- args) {
            var xx = x.split("=")
            mapParameters += (xx(0) -> xx(1))
        }

        var gameList: List[String] = List()
        if (mapParameters.contains("gameList")) {
            gameList = mapParameters("gameList").split(",").toList
        }

        var timings: List[String] = List()
        if (mapParameters.contains("timings")) {
            timings = mapParameters("timings").split(",").toList
        }else{
            timings = "a1,a7,a30,ac7,ac30".split(",").toList
        }

        if (mapParameters.contains("gameCode")) {
            gameList = mapParameters("gameCode") :: gameList
        }
        val logDate = mapParameters("logDate")

        val dataSource = mapParameters("dataSource")

        var dataFolder = "sdk_data"
        if (mapParameters.contains("dataFolder")) {
            dataFolder = mapParameters("dataFolder")
        }

        val conf = new SparkConf().setAppName("Device Report")
        conf.set("spark.hadoop.validateOutputSpecs", "false")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        gameList.foreach { gameCode =>
            val deviceConfig: scala.collection.mutable.Map[String, Map[String, String]] = DeviceConfig.getDeviceConfig(gameCode)
            val prePath = HOME_DIR + "/" + gameCode + "/ub/"  + dataFolder
            if (deviceConfig.size == 0) {

            } else {
                timings.foreach{ _timing =>
                    run(sc, sqlContext, deviceConfig, prePath, HOME_DIR, gameCode, logDate, _timing, dataSource)
                }
                /*
                run(sc, sqlContext, deviceConfig, prePath, HOME_DIR, gameCode, logDate, Constants.Timing.A1, dataSource)
                run(sc, sqlContext, deviceConfig, prePath, HOME_DIR, gameCode, logDate, Constants.Timing.A7, dataSource)
                run(sc, sqlContext, deviceConfig, prePath, HOME_DIR, gameCode, logDate, Constants.Timing.AC7, dataSource)
                run(sc, sqlContext, deviceConfig, prePath, HOME_DIR, gameCode, logDate, Constants.Timing.A30, dataSource)
                run(sc, sqlContext, deviceConfig, prePath, HOME_DIR, gameCode, logDate, Constants.Timing.AC30, dataSource)
                */
            }
        }

        sc.stop()
    }

    def run(sc: SparkContext, sqlContext: SQLContext, deviceConfig: scala.collection.mutable.Map[String, Map[String, String]], prePath: String, gslog: String, gameCode: String, logDate: String, timming: String, dataSource: String) {
        def isIntegerNumber(s: String): Boolean = (allCatch opt s.toInt).isDefined

        val activityPathList = DataUtils.getListFiles(prePath + "/activity_2", logDate, timming)
        val paymentPathList = DataUtils.getListFiles(prePath + "/payment_2", logDate, timming)
        val firstChargePathList = DataUtils.getListFiles(prePath + "/first_charge_2", logDate, timming)
        val newAccPathList = DataUtils.getListFiles(prePath + "/accregister_2", logDate, timming)

        Common.logger("timing:" + timming)
        Common.logger("activityPathList:" + activityPathList)
        Common.logger("paymentPathList:" + paymentPathList)
        Common.logger("firstChargePathList:" + firstChargePathList)
        Common.logger("newAccPathList:" + newAccPathList)


        var activityDf = sqlContext.read.parquet(activityPathList: _*).select("id", "device", "os", "os_version")
        activityDf = activityDf.sort(activityDf("id")).dropDuplicates(Seq("id"))
        val paymentDf = sqlContext.read.parquet(paymentPathList: _*).select("id", "net_amt", "device", "os", "os_version").cache

        var firstChargeDf: DataFrame = null
        var accRegisterDf: DataFrame = null
        var firstChargeRevenueDf: DataFrame = null
        if (!checkOldGame(gameCode)) {
            accRegisterDf = sqlContext.read.parquet(newAccPathList: _*).select("id", "device", "os", "os_version")
            firstChargeDf = sqlContext.read.parquet(firstChargePathList: _*).select("id", "device", "os", "os_version")
            firstChargeRevenueDf = paymentDf.as('pm).join(firstChargeDf.as('f),
                paymentDf("id") === firstChargeDf("id")).selectExpr("pm.net_amt as net_amt", "pm.id as id", "pm.device as device", "pm.os as os", "pm.os_version as os_version")
        }

        def checkOldGame(gameCode:String): Boolean = {
            if (oldGames.contains(gameCode)) {
                return true
            }
            false
            //true
        }



        def getValue1(metricMap: Map[String, String], metricValue: String, regexMap:Map[String, Regex]): String = {
            if (metricValue == "")
                return ""
            var isMatch = false
            var returnValue = ""
            metricMap.keys.foreach { key =>
                if (!isMatch) {
                    val value = metricMap(key)
                    val regex = regexMap(key)
                    returnValue = value
                    regex.findAllIn(metricValue).matchData.foreach { m =>
                        isMatch = true
                        value.split("\\{").foreach { index_1 =>
                            if (index_1.length() != 0) {
                                val index = index_1.substring(0, 1)
                                if (isIntegerNumber(index)) {
                                    val rs = Try {
                                        m.group(index.toInt)
                                    }
                                    var v = "0"
                                    if (rs.isSuccess) {
                                        v = m.group(index.toInt)
                                    }
                                    returnValue = returnValue.replace("{" + index + "}", v)
                                }
                            }
                        }
                    }
                }
            }
            if (!isMatch) {
                returnValue = "other"
            }
            returnValue
        }

        def getJsonString(data: Map[String, Long]): String = {
            var data_string_1 = "{"
            data.foreach { line =>
                val (key, value) = line
                if (key.toString != "" && value.toString != "") {
                    data_string_1 += "\"" + key + "\"" + ":" + "\"" + value + "\""
                    data_string_1 += ","
                }
            }
            if (data_string_1 != "{") {
                val data_string_2 = data_string_1.dropRight(1) + "}"
                return data_string_2
            }
            return NO_DATA
        }
        def getJsonStringD(data: Map[String, Double]): String = {
            var data_string_1 = "{"
            data.foreach { line =>
                val (key, value) = line
                if (key.toString != "" && value.toString != "") {
                    data_string_1 += "\"" + key + "\"" + ":" + "\"" + value + "\""
                    data_string_1 += ","
                }
            }
            if (data_string_1 != "{") {
                val data_string_2 = data_string_1.dropRight(1) + "}"
                return data_string_2
            }
            return NO_DATA
        }

        def getStringTsvFormat(dataString: String, game_code: String, log_date: String, dataSource: String, kpiId: String, current_date: String): String = {
            if (dataString != "") {
                val data_string = log_date + "\t" + game_code + "\t" + dataSource + "\t" + kpiId + "\t" + dataString + "\t" + current_date
                return data_string
            }
            NO_DATA
        }

        def getMetricValue(row: Row, data_type_1: String): String = {
            var returnValue: String = ""
            val b = Try {
                returnValue = data_type_1 match {
                    case "tel" => row.getAs("tel").toString
                    //case "os" => row.getAs("os").toString + " " + row.getAs("os_version").toString
                    case "os" => row.getAs("os").toString
                    case "nwk" => row.getAs("nwk").toString
                    case "device" => row.getAs("device").toString
                    case "sc" => row.getAs("sW").toString + "*" + row.getAs("sH").toString
                }
            }
            var dataReturn = "other"
            if (b.isSuccess) {
                dataReturn = returnValue.toLowerCase()
            }
            dataReturn.trim
        }

        def insertIntoDB(dataType: String, logDate: String, gameCode: String, dataSource: String, kpiId: String, revenueString: String, current_date: String): Unit = {
            var mysqlDB = new MysqlDB()
            var tableName = dataType + "_kpi"
            val sqlDelete = "delete from " + tableName + " where game_code = '" + gameCode + "'" +
                " and source = '" + dataSource + "'" +
                " and report_date = '" + logDate + "'" +
                " and kpi_id = " + kpiId
            mysqlDB.executeUpdate(sqlDelete, false)
            val sql = "insert into " + tableName + " (report_date, game_code, source, kpi_id, kpi_value, calc_date) values(" +
                "'" + logDate + "','" + gameCode + "','" + dataSource + "'," + kpiId + ",'" + revenueString + "','" + current_date + "')"
            Common.logger(sql)
            mysqlDB.executeUpdate(sql, false)
            mysqlDB.close()
        }

        def getRegexMap(sets: Map[String, String]) : Map[String, Regex] = {
            var regexMap:Map[String, Regex] = Map()
            sets.keys.foreach { key =>
                regexMap += (key -> key.r)
            }
            regexMap
        }
        val osConfig = Map(
            "android" -> "android",
            "ios" -> "ios",
            "iphone os" -> "ios"
        )
        def getValue(metricMap: Map[String, String], metricValue: String, regexMap:Map[String, Regex]): String = {
            var t = metricValue.toLowerCase
            if(osConfig.contains(t))
                osConfig(t)
            else
                "other"
        }

        val now = Calendar.getInstance().getTime()
        val log_date_format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val current_date = log_date_format.format(now)

        deviceConfig.keys.foreach { data_type_1 => //data_type_1 = tel, os, nwk...
            var number: Int = 0
            var data_map: Map[String, String] = Map()
            val sets: Map[String, String] = deviceConfig(data_type_1)

            var regexMap:Map[String, Regex] = getRegexMap(sets)

            val active = activityDf.map { row =>
                val metric = getMetricValue(row, data_type_1) // metric = Android4.2, Ios9.1, Viettel....
                val returnValue = getValue(sets, metric, regexMap) // returnValue = android4.2, iso9.1, viettel...
                (returnValue.toLowerCase(), row.getAs("id"): String)
            }

            val paying = paymentDf.map { row =>
                val metric = getMetricValue(row, data_type_1) // metric = Android4.2, Ios9.1, Viettel....
            val returnValue = getValue(sets, metric,regexMap) // returnValue = android4.2, iso9.1, viettel...
                (returnValue.toLowerCase(), row.getAs("id"): String)
            }

            val revenue = paymentDf.map { row =>
                val metric = getMetricValue(row, data_type_1) // metric = Android4.2, Ios9.1, Viettel....
            val returnValue = getValue(sets, metric,regexMap) // returnValue = android4.2, iso9.1, viettel...
                (returnValue.toLowerCase(), row.getAs("net_amt"): Double)
            }

            var firstChargeNumber: RDD[(String, String)] = null
            var accRegisterNumber: RDD[(String, String)] = null
            var firstChargeRevenue: RDD[(String, Double)] = null
            if (!checkOldGame(gameCode)) {
                firstChargeNumber = firstChargeDf.map { row =>
                    val metric = getMetricValue(row, data_type_1) // metric = Android4.2, Ios9.1, Viettel....
                val returnValue = getValue(sets, metric,regexMap) // returnValue = android4.2, iso9.1, viettel...
                    (returnValue.toLowerCase(), row.getAs("id"): String)
                }

                accRegisterNumber = accRegisterDf.map { row =>
                    val metric = getMetricValue(row, data_type_1) // metric = Android4.2, Ios9.1, Viettel....
                    val returnValue = getValue(sets, metric,regexMap) // returnValue = android4.2, iso9.1, viettel...
                    (returnValue.toLowerCase(), row.getAs("id"): String)
                }

                firstChargeRevenue = firstChargeRevenueDf.map { row =>
                    val metric = getMetricValue(row, data_type_1) // metric = Android4.2, Ios9.1, Viettel....
                    val returnValue = getValue(sets, metric,regexMap) // returnValue = android4.2, iso9.1, viettel...
                    (returnValue.toLowerCase(), row.getAs("net_amt"): Double)
                }
            }


            var kpiId: String = DataUtils.getKpiId("id", Constants.Kpi.ACTIVE, timming).toString
            val activeJsonString = getJsonString(active.distinct.countByKey.toMap)
            if (activeJsonString != NO_DATA) {
                insertIntoDB(data_type_1, logDate, gameCode, dataSource, kpiId, activeJsonString, current_date)
                val activeString = getStringTsvFormat(activeJsonString, gameCode, logDate, dataSource, kpiId, current_date)
                data_map += (number.toString -> activeString)
                number = number + 1
            }

            kpiId = DataUtils.getKpiId("id", Constants.Kpi.PAYING_USER, timming).toString
            val payingJsonString = getJsonString(paying.distinct.countByKey.toMap)
            if (payingJsonString != NO_DATA) {
                insertIntoDB(data_type_1, logDate, gameCode, dataSource, kpiId, payingJsonString, current_date)
                val payingString = getStringTsvFormat(activeJsonString, gameCode, logDate, dataSource, kpiId, current_date)
                data_map += (number.toString -> payingString)
                number = number + 1
            }

            kpiId = DataUtils.getKpiId("id", Constants.Kpi.NET_REVENUE, timming).toString
            val revenueJsonString = getJsonStringD(revenue.groupBy(_._1).mapValues(_.map(_._2).sum).collect().toMap)
            if (revenueJsonString != NO_DATA) {
                insertIntoDB(data_type_1, logDate, gameCode, dataSource, kpiId, revenueJsonString, current_date)
                val revenueString = getStringTsvFormat(revenueJsonString, gameCode, logDate, dataSource, kpiId, current_date)
                data_map += (number.toString -> revenueString)
                number = number + 1
            }

            if (!checkOldGame(gameCode)) {
                kpiId = DataUtils.getKpiId("id", Constants.Kpi.NEW_ACCOUNT_PLAYING,  timming).toString
                val accRegisterNJsonString = getJsonString(accRegisterNumber.distinct.countByKey.toMap)
                if (accRegisterNJsonString != NO_DATA) {
                    insertIntoDB(data_type_1, logDate, gameCode, dataSource, kpiId, accRegisterNJsonString, current_date)
                    val accRegisterNString = getStringTsvFormat(accRegisterNJsonString, gameCode, logDate, dataSource, kpiId, current_date)
                    data_map += (number.toString -> accRegisterNString)
                    number = number + 1
                }

                kpiId = DataUtils.getKpiId("id", Constants.Kpi.NEW_PAYING, timming).toString
                val firstChargeNJsonString = getJsonString(firstChargeNumber.distinct.countByKey.toMap)
                if (firstChargeNJsonString != NO_DATA) {
                    insertIntoDB(data_type_1, logDate, gameCode, dataSource, kpiId, firstChargeNJsonString, current_date)
                    val firstChargeNString = getStringTsvFormat(firstChargeNJsonString, gameCode, logDate, dataSource, kpiId, current_date)
                    data_map += (number.toString -> firstChargeNString)
                    number = number + 1
                }

                kpiId = DataUtils.getKpiId("id", Constants.Kpi.NEW_PAYING_NET_REVENUE, timming).toString
                val firstChargeRevenueJsonString = getJsonStringD(firstChargeRevenue.groupBy(_._1).mapValues(_.map(_._2).sum).collect().toMap)
                if (firstChargeRevenueJsonString != NO_DATA) {
                    insertIntoDB(data_type_1, logDate, gameCode, dataSource, kpiId, firstChargeRevenueJsonString, current_date)
                    val firstChargeRevenueString = getStringTsvFormat(firstChargeRevenueJsonString, gameCode, logDate, dataSource, kpiId, current_date)
                    data_map += (number.toString -> firstChargeRevenueString)
                    number = number + 1
                }
            }

            if (number != 0) {
                var stt = 0
                val array_size = data_map.keys.size
                val data_arr = new Array[String](array_size)
                data_map.keys.foreach { key =>
                    val data_a = data_map(key)
                    data_arr(stt) = data_a
                    stt = stt + 1
                }
                val file_path = gslog + "/" + gameCode + "/ub/sdk_report/" + timming + "/kpi_" + data_type_1 + "/" + logDate
                //sc.parallelize(data_arr).saveAsTextFile(file_path)
            }
        }

        paymentDf.unpersist()
    }
}





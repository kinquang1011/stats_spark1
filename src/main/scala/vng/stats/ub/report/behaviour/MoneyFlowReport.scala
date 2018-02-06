package vng.stats.ub.report.behaviour

import java.util.Calendar

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import vng.stats.ub.db.MysqlDB
import vng.stats.ub.utils.{Common, Constants, DateTimeUtils}
import scala.util.control.Breaks._
import scala.collection.immutable.HashMap


/**
 * Created by tuonglv on 07/10/2016.
 */
object MoneyFlowReport {

    var mapParameters: Map[String, String] = Map()
    var tableName = "custom_report"
    var reportCode = "add_spent_by_level"
    val mysqlDB = new MysqlDB()

    def main(args: Array[String]): Unit = {
        for (x <- args) {
            var xx = x.split("=")
            mapParameters += (xx(0) -> xx(1))
        }

        val conf = new SparkConf().setAppName("Money Flow Report::" + mapParameters("gameCode").toUpperCase)
        conf.set("spark.hadoop.validateOutputSpecs", "false")
        val sc = new SparkContext(conf)

        var levelConfig = getLevelGroupConfig(mapParameters("gameCode"))
        var moneyType = getMoneyTypeReport(mapParameters("gameCode"))
        var timings = getTimingsConfig(mapParameters("gameCode"),mapParameters("logDate"))

        addSpendByLevel(sc, levelConfig, moneyType, timings)


    }

    def getLevelGroupConfig(gameCode: String): Array[String] = {
        var arr: Array[String] = Array()
        gameCode match {
            case default =>
                arr = arr ++ Array("1-10")
                arr = arr ++ Array("11-15")
                arr = arr ++ Array("16-20")
                arr = arr ++ Array("21-25")
                arr = arr ++ Array("26-30")
                arr = arr ++ Array("31-35")
                arr = arr ++ Array("36-40")
        }
        arr
    }

    def getMaxLevelConfig(arrLevelConfig:Array[String]): Int ={
        var arrLevel:Array[Int] = Array()
        for(levelConfig <- arrLevelConfig){
            var t1 = levelConfig.split("-")
            var l1 = t1(0).toInt
            var l2 = t1(1).toInt
            arrLevel = arrLevel ++ Array(l1)
            arrLevel = arrLevel ++ Array(l2)
        }
        arrLevel.max
    }

    def getTimingsConfig(gameCode:String, logDate:String): Map[String, String] ={
        var timings: Map[String, String] = HashMap()

        var selectedDay = logDate
        var last3Day = DateTimeUtils.getDateDifferent(-3, logDate, Constants.TIMING, Constants.A1)
        var last7Day = DateTimeUtils.getDateDifferent(-7, logDate, Constants.TIMING, Constants.A1)
        var last30Day = DateTimeUtils.getDateDifferent(-30, logDate, Constants.TIMING, Constants.A1)

        gameCode match {
            case default =>
                timings += ("1" -> selectedDay)
                timings += ("3" -> last3Day)
                timings += ("7" -> last7Day)
                timings += ("30" -> last30Day)
                timings += ("all" -> "all")
        }
        timings
    }

    def getMoneyTypeReport(gameCode: String): String = {
        var moneyType = ""
        gameCode match {
            case default =>
                moneyType = "7"
        }
        moneyType
    }


    def addSpendByLevel(sc: SparkContext, levelConfigGroup: Array[String], moneyType: String, timings: Map[String, String]): Unit = {
        val sqlContext = new SQLContext(sc)

        var isSdk = false
        if (mapParameters.contains("isSdk") && mapParameters("isSdk") == "true") {
            isSdk = true
        }

        var logDate = mapParameters("logDate")
        var source = mapParameters("source")
        var gameCode = mapParameters("gameCode")

        var moneyFlowPath = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.MONEY_FLOW_OUTPUT_FOLDER, logDate, isSdk)
        var allMoneyFLowPath = moneyFlowPath.replace(logDate,"*")
        var moneyFlowDf = sqlContext.read.parquet(allMoneyFLowPath)
        moneyFlowDf.cache

        timings.keys.foreach { timing =>
            var groupByLevel: DataFrame = null
            if (timing != "all") {
                groupByLevel = moneyFlowDf.filter("log_date >='" + timings(timing) + "'").groupBy("level", "add_or_reduce").agg(sum("i_money").as('total))
            } else {
                groupByLevel = moneyFlowDf.groupBy("level", "add_or_reduce").agg(sum("i_money").as('total))
            }

            var dataMap: Map[String, Map[String,Double]] = HashMap()

            val now = Calendar.getInstance().getTime()
            val logDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val currentDateTime = logDateFormat.format(now)
            var maxLevel = getMaxLevelConfig(levelConfigGroup)

            groupByLevel.collect().foreach { row =>
                var level:Int = row.getAs("level")
                var addOrReduce:String = row.getAs("add_or_reduce").toString
                var addOrReduceKey:String = ""
                if(addOrReduce == Constants.MONEY_FLOW_ADD.toString){
                    addOrReduceKey = "add"
                }else{
                    addOrReduceKey = "spent"
                }
                var total:Double = row.getAs("total")
                breakable {
                    for (levelConfig <- levelConfigGroup) {
                        var t1 = levelConfig.split("-")
                        var l1 = t1(0).toInt
                        var l2 = t1(1).toInt

                        if ((level >= l1 && level <= l2) || level > maxLevel) {
                            var levelConfigKey = levelConfig
                            if(level > maxLevel){
                                levelConfigKey = level + "-" + level
                            }

                            var _total:Double = 0
                            var t2: Map[String, Double] = HashMap()
                            if (dataMap.contains(addOrReduceKey)) {
                                t2 = dataMap(addOrReduceKey)
                                var t: Map[String, Double] = dataMap(addOrReduceKey)
                                if (t.contains(levelConfigKey)) {
                                    _total = dataMap(addOrReduceKey)(levelConfigKey)
                                }
                            }
                            _total += total
                            Common.logger("level = " + level + ", levelConfigKey = " + levelConfigKey + ", levelConfig = " + levelConfig + ", total = " + _total)
                            t2 += (levelConfigKey -> _total)
                            dataMap += (addOrReduceKey -> t2)
                            break
                        }
                    }
                }
            }

            var jsonString = Common.hashMapToJson(dataMap)
            Common.logger(jsonString)


            var deleteMap: Map[String, String] = HashMap()
            deleteMap += ("game_code" -> gameCode)
            deleteMap += ("timing" -> timing)
            deleteMap += ("log_date" -> logDate)
            deleteMap += ("report_code" -> reportCode)
            deleteMap += ("source" -> source)

            var insertMap: Map[String, String] = HashMap()
            insertMap += ("game_code" -> gameCode)
            insertMap += ("timing" -> timing)
            insertMap += ("log_date" -> logDate)
            insertMap += ("report_code" -> reportCode)
            insertMap += ("report_value" -> jsonString)
            insertMap += ("calc_date" -> currentDateTime)
            insertMap += ("source" -> source)

            mysqlDB.insertOrUpdate(deleteMap, insertMap, tableName)
        }

        moneyFlowDf.unpersist()

        mysqlDB.close()
    }

}
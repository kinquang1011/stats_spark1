package vng.stats.ub.normalizer.format.v2

import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import vng.stats.ub.db.MysqlDB
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.utils.{Constants, DateTimeUtils, Common}
import org.apache.spark.sql.functions._
import scala.util.Try
import org.apache.spark.sql.functions.udf

/**
 * Created by tuonglv on 25/05/2016.
 */
class PaymentJsonFormatter(_gameCode:String, _logDate:String, _config: Array[FormatterConfig])
    extends  PaymentFormatter(_gameCode, _logDate, _config) {
    monitorCode="pmjs"
    override def format(sc: SparkContext): Unit = {
        val sqlContext = new SQLContext(sc)
        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
        sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")

        val formatTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        var finalData: DataFrame = null
        val formatConfig1 = config(0)
        val filePath1 = formatConfig1.filePath
        val extraTime = formatConfig1.extraTime
        val convertRate = formatConfig1.convertRate
        val increamentTime = udf((currentTime: String) =>
            formatTime.format(formatTime.parse(currentTime).getTime + extraTime)
        )
        var objDF1: DataFrame = null
        val b = Try {
            objDF1 = sqlContext.read.json(filePath1)
        }
        if (!b.isSuccess) {
            Common.logger("Exception in filepath = " + filePath1, "ERROR")
        }
        if (objDF1 != null && !objDF1.rdd.isEmpty()) {
            if (extraTime != 0L) {
                objDF1 = objDF1.withColumn("updatetime", increamentTime(objDF1("updatetime")))
            }
            var objDF2 = objDF1.filter(objDF1("resultCode") === "1").selectExpr("'" + gameCode + "' as game_code",
                "updatetime as log_date",
                "userID as id", "pmcID as pay_channel", "pmcNetChargeAmt * " + convertRate + " as net_amt",
                "pmcGrossChargeAmt * " + convertRate + " as gross_amt", "transactionID as trans_id")

            if (!objDF2.rdd.isEmpty()) {
                Common.logger("Data not null, path = " + filePath1)
                finalData = objDF2

                var formatConfig2: FormatterConfig = null
                val bC = Try {
                    formatConfig2 = config(1)
                }
                if (bC.isSuccess) {
                    Common.logger("bc is success")
                    val filePath2 = formatConfig2.filePath
                    var objDF3: DataFrame = null
                    val bb = Try {
                        objDF3 = sqlContext.read.json(filePath2)
                    }
                    if (!bb.isSuccess) {
                        Common.logger("Exception in filepath = " + filePath2, "ERROR")
                    }
                    if (objDF3 != null && !objDF3.rdd.isEmpty()) {
                        if (extraTime != 0L) {
                            objDF3 = objDF3.withColumn("updatetime", increamentTime(objDF3("updatetime")))
                        }
                        var objDF4 = objDF3.selectExpr("userID as id",
                            "package_name as package_name", "type as channel",
                            "device as device", "device_os as os", "os as os_version")
                        if (!objDF4.rdd.isEmpty()) {
                            if (finalData != null) {
                                objDF4 = objDF4.sort(objDF4("id")).dropDuplicates(Seq("id"))
                                val t1 = finalData.coalesce(1).as('pm).join(objDF4.coalesce(1).as('lg),
                                    finalData("id") === objDF4("id"), "left").selectExpr("pm.game_code", "pm.log_date", "lg.package_name", "pm.id",
                                        "lg.channel", "pm.pay_channel", "pm.net_amt", "pm.gross_amt", "pm.trans_id", "lg.device", "lg.os",
                                        "lg.os_version")
                                finalData = t1
                                Common.logger("finalData = t1")
                            }
                        }
                    }
                }
            }
        }
        if (finalData != null) {
            var oneDayBefore = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
            var oneDayAfter = DateTimeUtils.getDateDifferent(1, logDate, Constants.TIMING, Constants.A1)
            finalData.cache()
            var t1 = finalData.filter(finalData("log_date").contains(logDate))
            var t2 = finalData.filter(finalData("log_date").contains(oneDayBefore))
            var t3 = finalData.filter(!finalData("log_date").contains(oneDayBefore)
                and !finalData("log_date").contains(logDate)
                and !finalData("log_date").contains(oneDayAfter))

            val parquetField = getParquetField(Array("game_code", "log_date", "package_name", "id", "channel",
                "pay_channel", "net_amt", "gross_amt", "trans_id", "device", "os", "os_version"))

            t1 = t1.sort(t1("trans_id")).dropDuplicates(Seq("trans_id"))
            t1 = convertToParquetType(t1, parquetField)
            t1.coalesce(1).write.mode(writeMode).format(writeFormat).save(outputPath)

            /*var outputPathBefore = outputPath.replace(logDate, oneDayBefore)
            var dfb: DataFrame = null
            Common.logger("Try to read path = " + outputPathBefore)
            var b = Try {
                dfb = sqlContext.read.parquet(outputPathBefore)
            }
            if (b.isSuccess) {
                Common.logger("Read success, processing for one day ago")
                dfb.cache
                var count_1 = dfb.count
                t2 = convertToParquetType(t2, parquetField)
                dfb = dfb.unionAll(t2)
                dfb = dfb.sort(dfb("trans_id")).dropDuplicates(Seq("trans_id"))
                var count_2 = dfb.count

                if (count_1 != count_2) {
                    var message = "Day format = " + logDate + ", logType = payment," +
                        " day append = " + oneDayBefore + ", before lines = " + count_1 + ", after lines = " + count_2
                    insertMonitorLog(Constants.ERROR_CODE.PAYMENT_LOG_APPEND, message)
                    Common.logger(message)

                    dfb = convertToParquetType(dfb, parquetField)
                    dfb.coalesce(1).write.mode(writeMode).format(writeFormat).save(outputPathBefore)
                }
                dfb.unpersist()
            }else{
                Common.logger("Read fail, path = " + outputPathBefore)
            }

            if (t3 != null && !t3.rdd.isEmpty()) {
                var allDay = t3.selectExpr("date_format(log_date,\"yyyy-MM-dd\") as f_log_date").distinct
                allDay.coalesce(1).collect.foreach { line =>
                    var day = line.toString()
                    day = day.dropRight(1)
                    day = day.drop(1)
                    var appendDayPath = outputPath.replace(logDate, day)

                    var dfo: DataFrame = null
                    Common.logger("Try to read path = " + appendDayPath)
                    var c = Try {
                        dfo = sqlContext.read.parquet(appendDayPath)
                    }

                    if(c.isSuccess){
                        dfo.cache
                        var count_3 = dfo.count
                        var t3_1 = t3.filter(t3("log_date").contains(day))
                        t3_1 = convertToParquetType(t3_1, parquetField)
                        dfo = dfo.unionAll(t3_1)
                        dfo = dfo.sort(dfo("trans_id")).dropDuplicates(Seq("trans_id"))
                        var count_4 = dfo.count

                        if (count_3 != count_4) {
                            var message = "Day format = " + logDate + ", logType = payment," +
                                " day append = " + day + ", before lines = " + count_3 + ", after lines = " + count_4
                            insertMonitorLog(Constants.ERROR_CODE.PAYMENT_LOG_APPEND, message)
                            Common.logger(message)

                            dfo = convertToParquetType(dfo, parquetField)
                            dfo.coalesce(1).write.mode(writeMode).format(writeFormat).save(appendDayPath)
                        }
                        dfo.unpersist()
                    }else{
                        Common.logger("Read fail, path = " + appendDayPath)
                    }
                }
            }*/
            finalData.unpersist()
        } else {
            var message = "payment is null, write schema without data"
            Common.logger(message)
            writeSchemaWithoutData(sc, sqlContext)
        }
    }
}

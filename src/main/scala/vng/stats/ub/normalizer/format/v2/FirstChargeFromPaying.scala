package vng.stats.ub.normalizer.format.v2

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.utils.{Common, DateTimeUtils, Constants}

import scala.util.Try

/**
 * Created by tuonglv on 30/05/2016.
 */
class FirstChargeFromPaying(_gameCode:String, _logDate:String, _config: Array[FormatterConfig], _isSdkLog: Boolean = true)
    extends FirstChargeFormatter (_gameCode, _logDate, _config) {
    monitorCode = "fcfp"
    var totalAccPath = ""
    var totalAccBeforePath = ""
    var fieldsSelected: Array[String] = Array()

    setFieldsSelected(_config)
    var dateBeforeOneDay = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
    outputPath = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.FIRST_CHARGE_OUTPUT_FOLDER, logDate, _isSdkLog)
    totalAccBeforePath = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.TOTAL_ACC_PAID_OUTPUT_FOLDER, dateBeforeOneDay, _isSdkLog)
    totalAccPath = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.TOTAL_ACC_PAID_OUTPUT_FOLDER, logDate, _isSdkLog)

    def setTotalAccOutputFolder(_totalAccPaidPath: String): Unit = {
        totalAccPath = _totalAccPaidPath
    }

    def setTotalAccBeforeOutputFolder(_totalAccPaidBeforePath: String): Unit = {
        totalAccBeforePath = _totalAccPaidBeforePath
    }

    def setFieldsSelected(config: Array[FormatterConfig]): Unit = {
        config.foreach { formatConfig =>
            if (formatConfig.rel != null) {
                val rel = formatConfig.rel
                val fields = rel("fields").asInstanceOf[Map[String, Array[String]]]
                fields.keys.foreach { table =>
                    val f_arr = fields(table)
                    f_arr.foreach { f =>
                        fieldsSelected = fieldsSelected ++ Array(f)
                    }
                }
            }
        }
    }

    def appendTotalAcc(newAcc: DataFrame, path: String, sqlContext: SQLContext): Unit = {
        if (_isSdkLog == false)
            return
        var full: DataFrame = null
        var full1: DataFrame = null
        Try {
            Common.logger("read total paid acc before path: " + totalAccBeforePath)
            full = sqlContext.read.parquet(totalAccBeforePath)
        }
        if (full == null && newAcc != null) {
            full1 = newAcc.select("game_code", "log_date", "id")
        } else if (full != null && newAcc != null) {
            full1 = newAcc.select("game_code", "log_date", "id").unionAll(full)
        } else if (full != null && newAcc == null) {
            full1 = full
        }
        if (full1 == null) {
            Common.logger("Both total paid acc and payment are null")
        } else {
            full1.cache
            full1.count
            Common.logger("Total paid acc will be write in: " + totalAccPath)
            full1.coalesce(1).write.mode("overwrite").format(writeFormat).save(totalAccPath)
            full1.unpersist()
            Common.logger("Write done")
        }
    }

    override def writeParquet(finalData: DataFrame, fullSchemaArr: Array[String], sqlContext: SQLContext, sc: SparkContext): Unit = {
        if (finalData != null) {
            var write: DataFrame = null
            val finalData1 = finalData.sort(finalData("id")).dropDuplicates(Seq("id"))
            Common.logger("FinalData not null, data will be write in: " + outputPath)
            val parquetField = getParquetField(fieldsSelected)
            write = convertToParquetType(finalData1, parquetField)
            write.cache()
            if (!verifyData(write)) {
                Common.logger("verifyData return false")
                return
            }
            var b = Try {
                write.coalesce(1).write.mode(writeMode).format(writeFormat).save(outputPath)
            }
            if (b.isSuccess) {
                Common.logger("Write done")
                appendTotalAcc(finalData1, outputPath, sqlContext)
            } else {
                var message = "first charge is null, write schema without data"
                Common.logger(message)
                writeSchemaWithoutData(sc, sqlContext)
                appendTotalAcc(null, outputPath, sqlContext)
            }
            write.unpersist()
        } else {
            var message = "first charge is null, write schema without data"
            Common.logger(message)
            writeSchemaWithoutData(sc, sqlContext)
            appendTotalAcc(null, outputPath, sqlContext)
        }
    }

    override def verifyData(data: DataFrame): Boolean = {
        var b = firstChargeReset(data)
        //!b
        true
    }

    def firstChargeReset(data: DataFrame): Boolean = {
        var isReset = false
        var paymentPath = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.PAYMENT_OUTPUT_FOLDER, logDate, _isSdkLog)

        var paymentData:DataFrame = null
        var bb = Try {
            paymentData = _sqlContext.read.parquet(paymentPath)
        }
        if(bb.isFailure){
            return isReset
        }

        var a_c: Long = paymentData.select("id").distinct.count()
        var n_c = data.select("id").distinct.count()

        if (a_c == n_c) {
            var dateBeforeOneDay = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
            var firstBeforePath = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.FIRST_CHARGE_OUTPUT_FOLDER, dateBeforeOneDay, _isSdkLog)

            var t1:DataFrame = null
            var b = Try {
                t1 = _sqlContext.read.parquet(firstBeforePath)
            }
            if(b.isSuccess && t1.count() != 0){
                isReset = true
                var mess = "pu1 = npu1 = " + a_c.toString
                Common.logger(mess)
                insertMonitorLog(Constants.ERROR_CODE.FIRST_CHARGE_RESET, mess, Constants.ERROR_CODE.WARNING)
            }
        }
        isReset
    }
}
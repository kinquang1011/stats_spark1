package vng.stats.ub.normalizer.format.v2

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.utils.{Common, Constants, DateTimeUtils}

import scala.util.Try

/**
 * Created by tuonglv on 30/05/2016.
 */
class NewDeviceFromLogin(_gameCode:String, _logDate:String, _config: Array[FormatterConfig], _isSdkLog: Boolean = true)
    extends DeviceRegisterFormatter (_gameCode, _logDate, _config, _isSdkLog) {
    monitorCode = "ndfl"
    var totalAccPath = ""
    var totalAccBeforePath = ""
    var fieldsSelected: Array[String] = Array()

    setFieldsSelected(_config)
    var dateBeforeOneDay = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
    outputPath = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.DEVICE_REGISTER_OUTPUT_FOLDER, logDate, _isSdkLog)
    totalAccBeforePath = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.TOTAL_DEVICE_LOGIN_OUTPUT_FOLDER, dateBeforeOneDay, _isSdkLog)
    totalAccPath = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.TOTAL_DEVICE_LOGIN_OUTPUT_FOLDER, logDate, _isSdkLog)

    def setTotalAccOutputFolder(_totalAccLoginPath: String): Unit = {
        totalAccPath = _totalAccLoginPath
    }

    def setTotalAccBeforeOutputFolder(_totalAccLoginBeforePath: String): Unit = {
        totalAccBeforePath = _totalAccLoginBeforePath
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

    override def writeParquet(finalData: DataFrame, fullSchemaArr: Array[String], sqlContext: SQLContext, sc: SparkContext): Unit = {
        if (finalData != null) {
            var write: DataFrame = null
            val finalData1 = finalData.sort(finalData("did"), finalData("log_date")).dropDuplicates(Seq("did"))
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
                writeSchemaWithoutData(sc, sqlContext)
                appendTotalAcc(null, outputPath, sqlContext)
            }
            write.unpersist()
        } else {
            writeSchemaWithoutData(sc, sqlContext)
            appendTotalAcc(null, outputPath, sqlContext)
        }
    }

    def appendTotalAcc(newAcc: DataFrame, path: String, sqlContext: SQLContext): Unit = {
        
        var full: DataFrame = null
        var full1: DataFrame = null
        Try {
            Common.logger("read total login device before path: " + totalAccBeforePath)
            full = sqlContext.read.parquet(totalAccBeforePath)
        }
        if (full == null && newAcc != null) {
            full1 = newAcc.select("game_code", "log_date", "did", "id")
        } else if (full != null && newAcc != null) {
            full1 = newAcc.select("game_code", "log_date", "did", "id").unionAll(full)
        } else if (full != null && newAcc == null) {
            full1 = full
        }
        if (full1 == null) {
            Common.logger("Both total login device and activity are null")
        } else {
            Common.logger("FinalData not null, data will be write in: " + totalAccPath)
            full1.coalesce(1).write.mode("overwrite").format(writeFormat).save(totalAccPath)
            Common.logger("Write done")
        }
    }

    override def verifyData(data: DataFrame): Boolean = {
        accRegisterReset(data)
        true
    }

    def accRegisterReset(data: DataFrame): Boolean = {
        var isReset = false
        var activityPath = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.LOGIN_LOGOUT_OUTPUT_FOLDER, logDate, _isSdkLog)
        var activityData:DataFrame = null
        var bb = Try {
            activityData = _sqlContext.read.parquet(activityPath)
        }
        if(bb.isFailure){
            return isReset
        }
        var a_c: Long = activityData.select("did").distinct().count()
        var n_c = data.select("did").distinct().count()

        if (a_c == n_c) {
            var dateBeforeOneDay = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
            var firstBeforePath = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.DEVICE_REGISTER_OUTPUT_FOLDER, dateBeforeOneDay, _isSdkLog)

            var t1: DataFrame = null
            var b = Try {
                t1 = _sqlContext.read.parquet(firstBeforePath)
            }
            if (b.isSuccess && t1.count() != 0) {
                isReset = true
                var mess = "a1 = n1 = " + a_c.toString
                Common.logger(mess)
                insertMonitorLog(Constants.ERROR_CODE.DEVICE_REGISTER_RESET, mess, Constants.ERROR_CODE.WARNING)
            }
        }
        isReset
    }
}
package vng.stats.ub.normalizer.format.v2

import scala.util.Try

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.udf

import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.utils.Common
/**
 * Created by tuonglv on 25/05/2016.
 */
class LoginLogoutJsonFormatter(_gameCode:String, _logDate:String, _config: Array[FormatterConfig])
        extends LoginLogoutFormatter(_gameCode, _logDate, _config) {
    monitorCode="lljs"
    override def format(sc: SparkContext): Unit = {
        val sqlContext = new SQLContext(sc)
        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
        sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
        var finalData: DataFrame = null

        val formatTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        config.foreach { formatConfig =>
            val filePath = formatConfig.filePath
            val extraTime = formatConfig.extraTime
            var objDF1: DataFrame = null
            val b = Try {
                objDF1 = sqlContext.read.json(filePath)
            }
            val increamentTime = udf((currentTime: String) => {
                    try{
                        val date = formatTime.parse(currentTime)
                        val v = date.getTime + extraTime
                        formatTime.format(v)
                    } catch {
                        case e: Exception => {
                            Common.logger(currentTime + " is wrong!")
                            
                            throw new Exception
                        }
                    }
                }
            )
            if (objDF1 != null && !objDF1.rdd.isEmpty()) {
                if (extraTime != 0L) {
                    
                    objDF1 = objDF1.withColumn("updatetime", increamentTime(objDF1("updatetime")))
                }
                val objDF = objDF1.filter(objDF1("updatetime").contains(logDate)).selectExpr("'" + gameCode + "' as game_code",
                    "updatetime as log_date", "package_name as package_name", "userID as id", "device_id as did", "type as channel", "'login' as action", "device as device", "device_os as os", "os as os_version")
                finalData = objDF
            }
        }
        var write: DataFrame = null
        if (finalData != null) {
            val parquetField = getParquetField(Array("game_code", "log_date", "package_name", "id", "did", "channel", "action", "device", "os", "os_version"))
            write = convertToParquetType(finalData, parquetField)
            Common.logger("FinalData not null, data will be write in: " + outputPath)
            write.coalesce(1).write.mode(writeMode).format(writeFormat).save(outputPath)
            Common.logger("Write done")
        } else {
            var message = "loginlogout is null, write schema without data"
            Common.logger(message)
            writeSchemaWithoutData(sc,sqlContext)
        }
    }
}

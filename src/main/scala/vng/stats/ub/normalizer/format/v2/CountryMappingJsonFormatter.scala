package vng.stats.ub.normalizer.format.v2

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.utils.{Constants, Common}

import scala.util.Try

/**
 * Created by tuonglv on 25/05/2016.
 */
class CountryMappingJsonFormatter(_gameCode:String, _logDate:String, _config: Array[FormatterConfig])
    extends CountryMappingFormatter(_gameCode, _logDate, _config) {
    monitorCode="country_mapping_json"
    override def format(sc: SparkContext): Unit = {
        val sqlContext = new SQLContext(sc)
        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
        sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
        var finalData: DataFrame = null

        val formatTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

        config.foreach { formatConfig =>
            val configName = formatConfig.configName
            val filePath = formatConfig.filePath
            val extraTime = formatConfig.extraTime
            var objDF1: DataFrame = null
            val b = Try {
                objDF1 = sqlContext.read.json(filePath)
            }
            val increamentTime = udf((currentTime: String) =>
                formatTime.format(formatTime.parse(currentTime).getTime + extraTime)
            )
            if (objDF1 != null && !objDF1.rdd.isEmpty()) {
                if (extraTime != 0L) {
                    objDF1 = objDF1.withColumn("updatetime", increamentTime(objDF1("updatetime")))
                }
                var logType = ""
                if (configName == Constants.LOGIN_LOGOUT_TYPE) {
                    logType = Constants.LOGIN_LOGOUT_TYPE
                } else {
                    logType = Constants.PAYMENT_TYPE
                }
                var country_field = "'not_field'"
                if(hasColumn(objDF1, "detect_country")) {
                    country_field = "detect_country"
                }else if(hasColumn(objDF1, "country")) {
                    country_field = "country"
                }

                var objDF = objDF1.selectExpr("'" + gameCode + "' as game_code", "'" + logDate + "' as log_date",
                    "userID as id", "'" + logType + "' as log_type", country_field + " as country_code")
                objDF = objDF.sort(asc("id"),desc("country_code")).dropDuplicates(Seq("id"))
                if(finalData == null && objDF != null){
                    finalData = objDF
                }else if (finalData != null && objDF != null){
                    finalData = finalData.unionAll(objDF)
                }else{
                    Common.logger("Both finalData and objDF is null")
                }
            }
        }
        var write: DataFrame = null
        if (finalData != null) {
            val parquetField = getParquetField(Array("game_code", "log_date", "id", "log_type", "country_code"))
            write = convertToParquetType(finalData, parquetField)
            Common.logger("FinalData not null, data will be write in: " + outputPath)
            write.coalesce(1).write.mode(writeMode).format(writeFormat).save(outputPath)
            Common.logger("Write done")
        } else {
            var message = "country_mapping_json is null, write schema without data"
            Common.logger(message)
            writeSchemaWithoutData(sc,sqlContext)
        }
    }
}

package vng.stats.ub.normalizer.format.v1

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import vng.stats.ub.utils.Common

import scala.util.Try

/**
 * Created by tuonglv on 25/05/2016.
 */
class LoginLogoutJsonFormatter(_gameCode:String, _logDate:String, _config: Array[FormatterConfig])
        extends  LoginLogoutFormatter(_gameCode, _logDate, _config){

    override def format(sc: SparkContext): Unit = {
        val sqlContext = new SQLContext(sc)
        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
        sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
        var finalData: DataFrame = null
        config.foreach { formatConfig =>
            val filePath = formatConfig.filePath
            var objDF1: DataFrame = null
            val b = Try {
                objDF1 = sqlContext.read.json(filePath)
            }
            if(objDF1 != null && !objDF1.rdd.isEmpty()){
                val objDF = objDF1.filter(objDF1("updatetime").contains(logDate)).selectExpr("'" + gameCode + "' as game_code",
                    "updatetime as log_date", "userID as id", "device_id as did", "type as channel", "'login' as action")
                finalData = objDF
            }

        }
        val outputPath = Common.getOuputParquetPath(gameCode,outputFolder,logDate,isSdkLog)
        var write: DataFrame = null
        if (finalData != null) {
            val parquetField = getParquetField(Array("game_code", "log_date", "id", "did", "channel", "action"))
            write = convertToParquetType(finalData,parquetField)
            write.coalesce(1).write.mode(writeMode).format(writeFormat).save(outputPath)
        }else{
            val schemaWithoutData: DataFrame = sqlContext.createDataFrame(sc.emptyRDD[Row],getDefaultSchema)
            schemaWithoutData.coalesce(1).write.mode(writeMode).format(writeFormat).save(outputPath)
            println("Data is null, write schema without data, path = " + outputFolder)
        }
    }
}

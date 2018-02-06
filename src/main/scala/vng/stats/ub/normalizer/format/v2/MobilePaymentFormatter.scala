package vng.stats.ub.normalizer.format.v2

import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.utils.{Common, Constants}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row

/**
 * Created by tuonglv on 13/05/2016.
 */

class MobilePaymentFormatter(_gameCode:String, _logDate:String, _config: Array[FormatterConfig], _isSdkLog: Boolean = true)
    extends Formatter (_gameCode, _logDate, _config) {
    var schema: List[List[Any]]  = List(
        List(Constants.PAYMENT_FIELD.GAME_CODE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.LOG_DATE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.PACKAGE_NAME, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.SID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.ID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.RID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.LEVEL, Constants.DATA_TYPE_INTEGER, Constants.ENUM0),
        List(Constants.PAYMENT_FIELD.TRANS_ID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.CHANNEL, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.GROSS_AMT, Constants.DATA_TYPE_DOUBLE, Constants.ENUM0),
        List(Constants.PAYMENT_FIELD.NET_AMT, Constants.DATA_TYPE_DOUBLE, Constants.ENUM0),
        List(Constants.PAYMENT_FIELD.XU_INSTOCK, Constants.DATA_TYPE_LONG, Constants.ENUM0),
        List(Constants.PAYMENT_FIELD.XU_SPENT, Constants.DATA_TYPE_LONG, Constants.ENUM0),
        List(Constants.PAYMENT_FIELD.XU_TOPUP, Constants.DATA_TYPE_LONG, Constants.ENUM0),
        List(Constants.PAYMENT_FIELD.IP, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.DEVICE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.OS, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.OS_VERSION, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING)
    )
    monitorCode="mbp"
    setParquetSchema(schema)
    outputPath = Common.getOuputParquetPath(gameCode,Constants.PARQUET_2.PAYMENT_OUTPUT_FOLDER,logDate, _isSdkLog)
    def setOutputPath(_outputPath: String): Unit = {
        outputPath = _outputPath
    }
    
    override def writeParquet(finalData: DataFrame, fullSchemaArr: Array[String], sqlContext: SQLContext, sc: SparkContext): Unit = {
        if (finalData != null) {
            val finalData1 = finalData.coalesce(1).sort(finalData("id"), finalData("log_date")).dropDuplicates(Seq("id", "log_date"))
            finalData.cache()
            Common.logger("FinalData not null, data will be write in: " + outputPath)
            finalData1.write.mode(writeMode).format(writeFormat).save(outputPath)
            Common.logger("Write done")
            finalData1.unpersist()
        } else {
            var message = "mobile payment is null, write schema without data"
            insertMonitorLog(Constants.ERROR_CODE.PAYMENT_MOBILE_LOG_NULL, message)
            writeSchemaWithoutData(sc,sqlContext)
        }
    }
}
package vng.stats.ub.normalizer.format.v2

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.utils.Common
import org.apache.spark.sql.functions._
import scala.util.Try
import org.apache.spark.sql.functions.udf

/**
 * Created by tuonglv on 25/05/2016.
 */
class PaymentJsonHourlyFormatter(_gameCode:String, _logDate:String, _config: Array[FormatterConfig])
    extends  PaymentFormatter(_gameCode, _logDate, _config){
    monitorCode="pmjsh"
    override def format(sc: SparkContext): Unit = {
        val sqlContext = new SQLContext(sc)
        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
        sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")

        val formatTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        var finalData: DataFrame = null
        val formatConfig1 = config(0)
        val filePath1 = formatConfig1.filePath
        val extraTime = formatConfig1.extraTime
        val increamentTime = udf((currentTime: String) =>
            formatTime.format(formatTime.parse(currentTime).getTime + extraTime)
        )
        var objDF1: DataFrame = null
        val b = Try{
            objDF1 = sqlContext.read.json(filePath1)
        }
        if(!b.isSuccess){
            Common.logger("Exception in filepath = " + filePath1, "ERROR")
        }
        if(objDF1 != null && !objDF1.rdd.isEmpty()){

            if(extraTime!=0L){
                objDF1 = objDF1.withColumn("updatetime", increamentTime(objDF1("updatetime")))
            }
            val objDF2 = objDF1.filter(objDF1("updatetime").contains(logDate) and objDF1("resultCode") === "1").selectExpr("'" + gameCode + "' as game_code",
                "updatetime as log_date",
                "userID as id", "pmcID as channel", "pmcNetChargeAmt as net_amt",
                "pmcGrossChargeAmt as gross_amt", "transactionID as trans_id")
            if(!objDF2.rdd.isEmpty()){
                Common.logger("Data not null, path = " + filePath1)
                finalData = objDF2

                var formatConfig2: FormatterConfig = null
                val bC = Try {
                    formatConfig2 = config(1)
                }
                if(bC.isSuccess) {
                    Common.logger("bc is success")
                    val filePath2 = formatConfig2.filePath
                    var objDF3: DataFrame = null
                    val bb = Try {
                        objDF3 = sqlContext.read.parquet(filePath2)
                    }
                    if (!bb.isSuccess) {
                        Common.logger("Exception in filepath = " + filePath2, "ERROR")
                    }
                    if (objDF3 != null && !objDF3.rdd.isEmpty()) {
                        var objDF4 = objDF3
                        if (!objDF4.rdd.isEmpty()) {
                            if (finalData != null) {
                                objDF4 = objDF4.sort(objDF4("id")).dropDuplicates(Seq("id"))
                                val t1 = finalData.coalesce(1).as('pm).join(objDF4.coalesce(1).as('lg),
                                    finalData("id") === objDF4("id"), "left").selectExpr("pm.game_code", "pm.log_date", "lg.package_name", "pm.id",
                                        "pm.channel","pm.net_amt", "pm.gross_amt", "pm.trans_id", "lg.device", "lg.os",
                                        "lg.os_version")
                                finalData = t1
                                Common.logger("finalData = t1")
                            }
                        }
                    }
                }
            }
        }

        var write: DataFrame = null
        if (finalData != null) {
            val parquetField = getParquetField(Array("game_code","log_date", "package_name", "id", "channel", "net_amt", "gross_amt", "trans_id", "device", "os", "os_version"))
            write = convertToParquetType(finalData,parquetField)
            Common.logger("FinalData not null, data will be write in: " + outputPath)
            write.coalesce(1).write.mode(writeMode).format(writeFormat).save(outputPath)
            Common.logger("Write done")
        }else{
            writeSchemaWithoutData(sc,sqlContext)
        }
    }
}

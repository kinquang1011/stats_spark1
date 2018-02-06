package vng.stats.ub.normalizer.format.v2

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.utils.Common

import scala.util.Try

/**
 * Created by tuonglv on 25/05/2016.
 */
class PaymentJsonStonySeaFormatter(_gameCode:String, _logDate:String, _config: Array[FormatterConfig])
    extends  PaymentFormatter(_gameCode, _logDate, _config){

    override def format(sc: SparkContext): Unit = {
        val sqlContext = new SQLContext(sc)
        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
        sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")


        var finalData: DataFrame = null
        val formatConfig1 = config(0)
        val filePath1 = formatConfig1.filePath


        val extraTime = formatConfig1.extraTime
        val formatTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val increamentTime = udf((currentTime: String) =>
            formatTime.format(formatTime.parse(currentTime).getTime + extraTime)
        )

        val change75 = udf((oldValue: String) =>
            oldValue.toDouble*0.75
        )
        val change100 = udf((oldValue: String) =>
            oldValue
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
            var objDF2 = objDF1.filter(objDF1("updatetime").contains(logDate) and objDF1("resultCode") === "1").selectExpr("'" + gameCode + "' as game_code",
                "updatetime as log_date",
                "userID as id", "pmcID as pay_channel", "pmcNetChargeAmt as net_amt",
                "pmcGrossChargeAmt as gross_amt", "transactionID as trans_id", "Cost as cost")

            /*

            // from 2016-07-20 00:00:00 to 2016-07-25 12:00:00
            objDF2 = objDF2.where("log_date <= '2016-07-25 12:00:00'")
            var objDF2_1 = objDF2.filter(objDF2("pay_channel") === "PLAY_STORE_playstore" or objDF2("pay_channel") === "APP_STORE_appstore")
            objDF2_1=objDF2_1.withColumn("gross_amt",change100(objDF2_1("cost")))
            objDF2_1=objDF2_1.withColumn("net_amt",change75(objDF2_1("cost")))

            var objDF2_2 = objDF2.filter(objDF2("pay_channel") !== "PLAY_STORE_playstore").filter(objDF2("pay_channel") !== "APP_STORE_appstore")

            objDF2 = objDF2_1.unionAll(objDF2_2)
*/
            //from 2016-07-25 12:00:01 to 2016-07-25 23:59:59
            objDF2 = objDF2.where("log_date > '2016-07-25 12:00:00'")


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
                        objDF3 = sqlContext.read.json(filePath2)
                    }
                    if (!bb.isSuccess) {
                        Common.logger("Exception in filepath = " + filePath2, "ERROR")
                    }
                    if (objDF3 != null && !objDF3.rdd.isEmpty()) {
                        if(extraTime!=0L){
                            objDF3 = objDF3.withColumn("updatetime", increamentTime(objDF3("updatetime")))
                        }
                        var objDF4 = objDF3.selectExpr("userID as id",
                            "package_name as package_name","type as channel",
                            "device as device", "device_os as os", "os as os_version")

                        if (!objDF4.rdd.isEmpty()) {
                            if (finalData != null) {
                                objDF4 = objDF4.sort(objDF4("id")).dropDuplicates(Seq("id"))
                                val t1 = finalData.coalesce(1).as('pm).join(objDF4.coalesce(1).as('lg),

                                finalData("id") === objDF4("id"), "left").selectExpr("pm.game_code", "pm.log_date", "lg.package_name", "pm.id",
                                    "lg.channel", "pm.pay_channel","pm.net_amt", "pm.gross_amt", "pm.trans_id", "lg.device", "lg.os",
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
            Common.logger("FinalData not null, data will be write in: " + outputPath)
            val parquetField = getParquetField(Array("game_code", "log_date", "package_name", "id", "channel",
                "pay_channel", "net_amt", "gross_amt", "trans_id", "device", "os", "os_version"))

            finalData = finalData.sort(finalData("trans_id")).dropDuplicates(Seq("trans_id"))
            write = convertToParquetType(finalData,parquetField)
            write.coalesce(1).write.mode("append").format(writeFormat).save(outputPath)
        }else{
            writeSchemaWithoutData(sc,sqlContext)
        }
    }
}

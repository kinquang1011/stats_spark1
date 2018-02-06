package vng.stats.ub.normalizer.format.v1

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import vng.stats.ub.utils.Common

import scala.util.Try

/**
 * Created by tuonglv on 25/05/2016.
 */
class PaymentJsonFormatter(_gameCode:String, _logDate:String, _config: Array[FormatterConfig])
    extends  PaymentFormatter(_gameCode, _logDate, _config){

    override def format(sc: SparkContext): Unit = {
        val sqlContext = new SQLContext(sc)
        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
        sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")

        var finalData: DataFrame = null

        val formatConfig1 = config(0)
        //val formatConfig2 = config(1)

        val filePath1 = formatConfig1.filePath
        //val filePath2 = formatConfig2.filePath

        var objDF1: DataFrame = null
        val b = Try{
            objDF1 = sqlContext.read.json(filePath1)
        }
        if(!b.isSuccess){
            println("Exception in filepath = " + filePath1)
        }

        if(objDF1 != null && !objDF1.rdd.isEmpty()){
            val objDF2 = objDF1.filter(objDF1("updatetime").contains(logDate) and objDF1("resultCode") === "1").selectExpr("'" + gameCode + "' as game_code","updatetime as log_date",
                "userID as id", "pmcID as channel", "pmcNetChargeAmt as net_amt",
                "pmcGrossChargeAmt as gross_amt", "transactionID as trans_id")
            if(!objDF2.rdd.isEmpty()){
                finalData = objDF2
            }
        }

        /*
        var objDF3: DataFrame = null
        val bb = Try{
            objDF3 = sqlContext.read.json(filePath2)
        }
        if(!bb.isSuccess){
            println("Exception in filepath = " + filePath2)
        }

        if(objDF3 != null && !objDF3.rdd.isEmpty()) {
            val objDF4 = objDF3.filter(objDF3("updatetime").contains(logDate) and objDF3("gameresultCode") === "1").selectExpr("updatetime as log_date",
                "userID as id", "'wallet' as channel", "cast (Cost as double)*100 as net_amt",
                "cast (Cost as double)*100 as gross_amt", "transactionID as trans_id")

            if(!objDF4.rdd.isEmpty()){
                if(finalData == null){
                    finalData = objDF4
                }else{
                    finalData = finalData.unionAll(objDF4)
                }
            }
        }
        */

        val outputPath = Common.getOuputParquetPath(gameCode,outputFolder,logDate,isSdkLog)
        var write: DataFrame = null
        if (finalData != null) {
            val parquetField = getParquetField(Array("game_code","log_date", "id", "channel", "net_amt", "gross_amt", "trans_id"))
            write = convertToParquetType(finalData,parquetField)
            write.coalesce(1).write.mode(writeMode).format(writeFormat).save(outputPath)
        }else{
            val schemaWithoutData: DataFrame = sqlContext.createDataFrame(sc.emptyRDD[Row],getDefaultSchema)
            schemaWithoutData.coalesce(1).write.mode(writeMode).format(writeFormat).save(outputPath)
            println("Data is null, write schema without data, path = " + outputFolder)
        }

    }
}

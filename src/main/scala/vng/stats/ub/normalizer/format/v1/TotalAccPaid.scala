package vng.stats.ub.normalizer.format.v1

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import vng.stats.ub.utils.{Common, Constants}

/**
 * Created by tuonglv on 30/05/2016.
 */
class TotalAccPaid(_gameCode:String, _logDate:String, _config: Array[FormatterConfig])
    extends Formatter (_gameCode, _logDate, _config) {
    var schema: List[List[Any]] = List(
        List(Constants.ACC_REGISTER_FIELD.GAME_CODE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.ACC_REGISTER_FIELD.LOG_DATE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.ACC_REGISTER_FIELD.ID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING)
    )

    setParquetSchema(schema)
    setOutputFolder(Constants.PARQUET.TOTAL_ACC_PAID_OUTPUT_FOLDER)

    override def writeParquet(finalData: DataFrame, fullSchemaArr: Array[String], sqlContext: SQLContext, sc: SparkContext): Unit = {
        var outputPath = Common.getOuputParquetPath(gameCode,outputFolder,logDate,isSdkLog)
        if (finalData != null) {
            val finalData1 = finalData.select("id").distinct
            val finalData2 = finalData1.selectExpr("'" + gameCode + "' as game_code", "'" + logDate + "' as log_date", "id as id")
            finalData2.distinct.coalesce(1).write.mode(writeMode).format(writeFormat).save(outputPath)
        } else {
            val schemaWithoutData: DataFrame = sqlContext.createDataFrame(sc.emptyRDD[Row],getDefaultSchema)
            schemaWithoutData.coalesce(1).write.mode(writeMode).format(writeFormat).save(outputPath)
            println("Data is null, write schema without data, path = " + outputFolder)
        }
    }
}
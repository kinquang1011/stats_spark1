package vng.stats.ub.normalizer.format.v1

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import vng.stats.ub.utils.{Common, Constants, DateTimeUtils}

import scala.util.Try

/**
 * Created by tuonglv on 30/05/2016.
 */
class FirstChargeFromPaying(_gameCode:String, _logDate:String, _config: Array[FormatterConfig])
    extends Formatter (_gameCode, _logDate, _config) {
    var schema: List[List[Any]]  = List(
        List(Constants.FIRST_CHARGE.GAME_CODE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.FIRST_CHARGE.LOG_DATE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.FIRST_CHARGE.ID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING)
    )

    setParquetSchema(schema)
    setOutputFolder(Constants.PARQUET.FIRST_CHARGE_OUTPUT_FOLDER)

    override def writeParquet(finalData: DataFrame, fullSchemaArr: Array[String], sqlContext: SQLContext, sc: SparkContext): Unit = {
        var outputPath= Common.getOuputParquetPath(gameCode,outputFolder,logDate, isSdkLog)
        if (finalData != null) {
            val finalData1 = finalData.sort(finalData("id")).dropDuplicates(Seq("id"))
            finalData1.coalesce(1).write.mode(writeMode).format(writeFormat).save(outputPath)
            appendTotalAcc(finalData1,outputPath,sqlContext)
        } else {
            var schemaWithoutData: DataFrame = sqlContext.createDataFrame(sc.emptyRDD[Row],getDefaultSchema)
            schemaWithoutData.coalesce(1).write.mode(writeMode).format(writeFormat).save(outputPath)
            println("Data is null, write schema without data, path = " + outputFolder)
        }
    }
    def appendTotalAcc(newAcc: DataFrame, path:String, sqlContext:SQLContext): Unit = {
        var dateBeforeOneDay = DateTimeUtils.getDateDifferent(-1,logDate,Constants.TIMING,Constants.A1)
        var path1 = Common.getOuputParquetPath(gameCode, Constants.PARQUET.TOTAL_ACC_PAID_OUTPUT_FOLDER, dateBeforeOneDay, isSdkLog)
        var full:DataFrame = null
        var full1:DataFrame = null
        Try{ full=sqlContext.read.parquet(path1)}
        if(full==null){
            full1 = newAcc.select("game_code","log_date","id")
        } else{
            full1 = newAcc.select("game_code","log_date","id").unionAll(full)
        }
        full1.coalesce(1).write.mode(writeMode).format(writeFormat).save(Common.getOuputParquetPath(gameCode, Constants.PARQUET.TOTAL_ACC_PAID_OUTPUT_FOLDER, logDate, isSdkLog))
    }
}
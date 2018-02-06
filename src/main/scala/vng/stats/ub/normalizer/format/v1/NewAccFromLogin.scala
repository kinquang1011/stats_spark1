package vng.stats.ub.normalizer.format.v1

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import vng.stats.ub.utils.{Common, Constants, DateTimeUtils}

import scala.util.Try

/**
 * Created by tuonglv on 30/05/2016.
 */
class NewAccFromLogin(_gameCode:String, _logDate:String, _config: Array[FormatterConfig])
    extends Formatter (_gameCode, _logDate, _config) {
    var schema: List[List[Any]]  = List(
        List(Constants.ACC_REGISTER_FIELD.GAME_CODE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.ACC_REGISTER_FIELD.LOG_DATE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.ACC_REGISTER_FIELD.SID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.ACC_REGISTER_FIELD.ID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.ACC_REGISTER_FIELD.IP, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING)
    )

    setParquetSchema(schema)
    setOutputFolder(Constants.PARQUET.ACC_REGISTER_OUTPUT_FOLDER)

    override def writeParquet(finalData: DataFrame, fullSchemaArr: Array[String], sqlContext: SQLContext, sc: SparkContext): Unit = {
        var outputPath = Common.getOuputParquetPath(gameCode,outputFolder,logDate,isSdkLog)
        if (finalData != null) {
            val finalData1 = finalData.sort(finalData("id")).dropDuplicates(Seq("id"))
            finalData1.coalesce(1).write.mode(writeMode).format(writeFormat).save(outputPath)
            appendTotalAcc(finalData1,outputPath,sqlContext)
        } else {
            val schemaWithoutData: DataFrame = sqlContext.createDataFrame(sc.emptyRDD[Row],getDefaultSchema)
            schemaWithoutData.coalesce(1).write.mode(writeMode).format(writeFormat).save(outputPath)
            println("Data is null, write schema without data, path = " + outputFolder)
        }
    }
    def appendTotalAcc(newAcc: DataFrame, path:String, sqlContext:SQLContext): Unit = {
        val dateBeforeOneDay = DateTimeUtils.getDateDifferent(-1,logDate,Constants.TIMING,Constants.A1)
        val path1 = Common.getOuputParquetPath(gameCode, Constants.PARQUET.TOTAL_ACC_LOGIN_OUTPUT_FOLDER, dateBeforeOneDay, isSdkLog)
        var full:DataFrame = null
        var full1:DataFrame = null
        Try{ full=sqlContext.read.parquet(path1)}
        if(full==null){
            full1 = newAcc.select("game_code","log_date","id")
        } else{
            full1 = newAcc.select("game_code","log_date","id").unionAll(full)
        }
        full1.coalesce(1).write.mode(writeMode).format(writeFormat).save(Common.getOuputParquetPath(gameCode, Constants.PARQUET.TOTAL_ACC_LOGIN_OUTPUT_FOLDER, logDate, isSdkLog))
    }
}
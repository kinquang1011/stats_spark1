package vng.stats.ub.normalizer.format.v2

import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.utils.{Common, Constants}

/**
 * Created by vinhdp on 05/08/2016.
 */

class TotalAccLoginFormatter(_gameCode:String, _logDate:String, _config: Array[FormatterConfig], _isSdkLog: Boolean = true)
    extends Formatter (_gameCode, _logDate, _config) {
    var schema: List[List[Any]]  = List(
        List(Constants.TOTAL_ACC_LOGIN.GAME_CODE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.TOTAL_ACC_LOGIN.LOG_DATE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.TOTAL_ACC_LOGIN.ID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING)

    )
    monitorCode="tal"
    setParquetSchema(schema)
    outputPath = Common.getOuputParquetPath(gameCode,Constants.PARQUET_2.TOTAL_ACC_LOGIN_OUTPUT_FOLDER,logDate, _isSdkLog)
    def setOutputPath(_outputPath: String): Unit = {
        outputPath = _outputPath
    }
}
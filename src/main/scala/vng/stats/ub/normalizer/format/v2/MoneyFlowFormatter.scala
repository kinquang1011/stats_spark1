package vng.stats.ub.normalizer.format.v2

import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.utils.{Common, Constants}

/**
 * Created by tuonglv on 13/05/2016.
 */

class MoneyFlowFormatter(_gameCode:String, _logDate:String, _config: Array[FormatterConfig], _isSdkLog: Boolean = true)
    extends Formatter (_gameCode, _logDate, _config) {
    var schema: List[List[Any]]  = List(
        List(Constants.MONEY_FLOW_FIELD.GAME_CODE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.MONEY_FLOW_FIELD.LOG_DATE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.MONEY_FLOW_FIELD.SID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.MONEY_FLOW_FIELD.ID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.MONEY_FLOW_FIELD.RID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.MONEY_FLOW_FIELD.LEVEL, Constants.DATA_TYPE_INTEGER, Constants.ENUM0),
        List(Constants.MONEY_FLOW_FIELD.I_MONEY, Constants.DATA_TYPE_DOUBLE, Constants.ENUM0),
        List(Constants.MONEY_FLOW_FIELD.MONEY_AFTER, Constants.DATA_TYPE_DOUBLE, Constants.ENUM0),
        List(Constants.MONEY_FLOW_FIELD.MONEY_TYPE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.MONEY_FLOW_FIELD.REASON, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.MONEY_FLOW_FIELD.SUB_REASON, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.MONEY_FLOW_FIELD.ADD_OR_REDUCE, Constants.DATA_TYPE_INTEGER, Constants.ENUM0)

    )
    monitorCode="mflow"
    setParquetSchema(schema)
    outputPath = Common.getOuputParquetPath(gameCode,Constants.PARQUET_2.MONEY_FLOW_OUTPUT_FOLDER,logDate, _isSdkLog)
    def setOutputPath(_outputPath: String): Unit = {
        outputPath = _outputPath
    }
}
package vng.stats.ub.normalizer.format.v1

import vng.stats.ub.utils.Constants
/**
 * Created by vinhdp on 13/07/2016.
 */

class CcuFormatter(_gameCode:String, _logDate:String, _config: Array[FormatterConfig])
    extends Formatter (_gameCode, _logDate, _config) {
    var schema: List[List[Any]]  = List(
        List(Constants.CCU.GAME_CODE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.CCU.LOG_DATE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.CCU.SID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.CCU.OS, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.CCU.CHANNEL, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.CCU.CCU, Constants.DATA_TYPE_LONG, Constants.ENUM0)
    )

    setParquetSchema(schema)
    setOutputFolder(Constants.PARQUET.CCU_OUTPUT_FOLDER)
}
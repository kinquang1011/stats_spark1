package vng.stats.ub.normalizer.format.v1

import vng.stats.ub.utils.Constants

/**
 * Created by tuonglv on 20/05/2016.
 */
class GetAccRegisterFromNewRole(_gameCode:String, _logDate:String, _config: Array[FormatterConfig])
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

}
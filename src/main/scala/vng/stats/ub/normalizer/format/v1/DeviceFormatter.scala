package vng.stats.ub.normalizer.format.v1

import vng.stats.ub.utils.Constants
/**
 * Created by tuonglv on 13/05/2016.
 */

class DeviceFormatter(_gameCode:String, _logDate:String, _config: Array[FormatterConfig])
    extends Formatter (_gameCode, _logDate, _config) {
    var schema: List[List[Any]]  = List(
        List(Constants.DEVICE_FIELD.GAME_CODE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.DEVICE_FIELD.LOG_DATE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.DEVICE_FIELD.SID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.DEVICE_FIELD.ID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.DEVICE_FIELD.OS, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.DEVICE_FIELD.DTY, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.DEVICE_FIELD.TEL, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.DEVICE_FIELD.NWK, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.DEVICE_FIELD.S_WIDTH, Constants.DATA_TYPE_INTEGER, Constants.ENUM0),
        List(Constants.DEVICE_FIELD.S_HEIGHT, Constants.DATA_TYPE_INTEGER, Constants.ENUM0)
        )

    setParquetSchema(schema)
    setOutputFolder(Constants.PARQUET.DEVICE_OUTPUT_FOLDER)
}
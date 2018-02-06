package vng.stats.ub.normalizer.format.v2

import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.utils.{Common, Constants}

/**
 * Created by tuonglv on 13/05/2016.
 */

class LoginLogoutFormatter(_gameCode:String, _logDate:String, _config: Array[FormatterConfig], _isSdkLog: Boolean = true)
    extends Formatter (_gameCode, _logDate, _config) {
    var schema: List[List[Any]]  = List(
        List(Constants.LOGIN_LOGOUT_FIELD.GAME_CODE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.LOGIN_LOGOUT_FIELD.LOG_DATE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.LOGIN_LOGOUT_FIELD.PACKAGE_NAME, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.LOGIN_LOGOUT_FIELD.SID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.LOGIN_LOGOUT_FIELD.ID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.LOGIN_LOGOUT_FIELD.RID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.LOGIN_LOGOUT_FIELD.DID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.LOGIN_LOGOUT_FIELD.ACTION, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.LOGIN_LOGOUT_FIELD.CHANNEL, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.LOGIN_LOGOUT_FIELD.ONLINE_TIME, Constants.DATA_TYPE_LONG, Constants.ENUM0),
        List(Constants.LOGIN_LOGOUT_FIELD.LEVEL, Constants.DATA_TYPE_INTEGER, Constants.ENUM0),
        List(Constants.LOGIN_LOGOUT_FIELD.IP, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.LOGIN_LOGOUT_FIELD.DEVICE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.LOGIN_LOGOUT_FIELD.OS, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.LOGIN_LOGOUT_FIELD.OS_VERSION, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING)
    )
    monitorCode="ll"
    setParquetSchema(schema)
    outputPath = Common.getOuputParquetPath(gameCode,Constants.PARQUET_2.LOGIN_LOGOUT_OUTPUT_FOLDER,logDate, _isSdkLog)
    def setOutputPath(_outputPath: String): Unit = {
        outputPath = _outputPath
    }
}
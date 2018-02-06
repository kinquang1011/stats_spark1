package vng.stats.ub.normalizer.format.v2

import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.utils.{Common, Constants}

class DeviceFirstChargeFormatter(_gameCode:String, _logDate:String, _config: Array[FormatterConfig], _isSdkLog: Boolean = true)
    extends Formatter (_gameCode, _logDate, _config) {
    var schema: List[List[Any]]  = List(
        List(Constants.DEVICE_FIRSTCHARGE_FIELD.GAME_CODE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.DEVICE_FIRSTCHARGE_FIELD.LOG_DATE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.DEVICE_FIRSTCHARGE_FIELD.PACKAGE_NAME, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.DEVICE_FIRSTCHARGE_FIELD.CHANNEL, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.DEVICE_FIRSTCHARGE_FIELD.PAY_CHANNEL, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.DEVICE_FIRSTCHARGE_FIELD.SID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.DEVICE_FIRSTCHARGE_FIELD.DID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.DEVICE_FIRSTCHARGE_FIELD.ID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.DEVICE_FIRSTCHARGE_FIELD.IP, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.DEVICE_FIRSTCHARGE_FIELD.DEVICE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.DEVICE_FIRSTCHARGE_FIELD.OS, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.DEVICE_FIRSTCHARGE_FIELD.OS_VERSION, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING)
    )

    monitorCode="dfcf"
    setParquetSchema(schema)
    outputPath = Common.getOuputParquetPath(gameCode,Constants.PARQUET_2.DEVICE_FIRST_CHARGE_OUTPUT_FOLDER,logDate, _isSdkLog)
    def setOutputPath(_outputPath: String): Unit = {
        outputPath = _outputPath
    }
}
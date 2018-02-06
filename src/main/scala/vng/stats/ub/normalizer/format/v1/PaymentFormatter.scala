package vng.stats.ub.normalizer.format.v1

import vng.stats.ub.utils.Constants
/**
 * Created by tuonglv on 13/05/2016.
 */

class PaymentFormatter(_gameCode:String, _logDate:String, _config: Array[FormatterConfig])
    extends Formatter (_gameCode, _logDate, _config) {
    var schema: List[List[Any]]  = List(
        List(Constants.PAYMENT_FIELD.GAME_CODE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.LOG_DATE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.SID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.ID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.RID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.LEVEL, Constants.DATA_TYPE_INTEGER, Constants.ENUM0),
        List(Constants.PAYMENT_FIELD.TRANS_ID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.CHANNEL, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.GROSS_AMT, Constants.DATA_TYPE_DOUBLE, Constants.ENUM0),
        List(Constants.PAYMENT_FIELD.NET_AMT, Constants.DATA_TYPE_DOUBLE, Constants.ENUM0),
        List(Constants.PAYMENT_FIELD.XU_INSTOCK, Constants.DATA_TYPE_LONG, Constants.ENUM0),
        List(Constants.PAYMENT_FIELD.XU_SPENT, Constants.DATA_TYPE_LONG, Constants.ENUM0),
        List(Constants.PAYMENT_FIELD.XU_TOPUP, Constants.DATA_TYPE_LONG, Constants.ENUM0),
        List(Constants.PAYMENT_FIELD.IP, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING)
    )

    setParquetSchema(schema)
    setOutputFolder(Constants.PARQUET.PAYMENT_OUTPUT_FOLDER)
}
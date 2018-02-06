package vng.stats.ub.normalizer.format.v2

import org.apache.spark.sql.{SQLContext, DataFrame}
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.utils.{Common, Constants}
import org.apache.spark.sql.functions._
/**
 * Created by tuonglv on 13/05/2016.
 */

class PaymentFormatter(_gameCode:String, _logDate:String, _config: Array[FormatterConfig], _isSdkLog: Boolean = true)
    extends Formatter (_gameCode, _logDate, _config) {
    var schema: List[List[Any]] = List(
        List(Constants.PAYMENT_FIELD.GAME_CODE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.LOG_DATE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.PACKAGE_NAME, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.SID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.ID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.RID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.LEVEL, Constants.DATA_TYPE_INTEGER, Constants.ENUM0),
        List(Constants.PAYMENT_FIELD.TRANS_ID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.CHANNEL, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.PAY_CHANNEL, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.GROSS_AMT, Constants.DATA_TYPE_DOUBLE, Constants.ENUM0),
        List(Constants.PAYMENT_FIELD.NET_AMT, Constants.DATA_TYPE_DOUBLE, Constants.ENUM0),
        List(Constants.PAYMENT_FIELD.XU_INSTOCK, Constants.DATA_TYPE_LONG, Constants.ENUM0),
        List(Constants.PAYMENT_FIELD.XU_SPENT, Constants.DATA_TYPE_LONG, Constants.ENUM0),
        List(Constants.PAYMENT_FIELD.XU_TOPUP, Constants.DATA_TYPE_LONG, Constants.ENUM0),
        List(Constants.PAYMENT_FIELD.IP, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.DEVICE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.OS, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.OS_VERSION, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.PAYMENT_FIELD.DID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING)
    )//
    monitorCode = "pm"
    setParquetSchema(schema)
    outputPath = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.PAYMENT_OUTPUT_FOLDER, logDate, _isSdkLog)

    def setOutputPath(_outputPath: String): Unit = {
        outputPath = _outputPath
    }

    override def verifyData(data: DataFrame): Boolean = {
        var b = checkLogDuplicateError(data)
        if(!b){
            checkLogDuplicateWarning(data)
        }
        !b
    }

    def checkLogDuplicateError(data: DataFrame): Boolean = {
        var notDup: Boolean = false
        var limit = 10
        var checkDup = data.select("log_date", "id", "net_amt").sort("log_date", "id").limit(limit).collect().toArray
        var getSize = checkDup.size
        if (getSize == limit || (getSize != 0 && getSize % 2 == 0)) {
            var i = 0
            var needSame = true

            var continuesUser = true
            while (i < getSize - 1 && !notDup) {
                var t1 = checkDup(i)
                var t2 = checkDup(i + 1)
                if (t1(0) == t2(0) && t1(1) == t2(1) && t1(2) == t2(2)) {
                    needSame = false
                } else if (needSame) {
                    continuesUser = false
                    notDup = true
                } else {
                    continuesUser = false
                    needSame = true
                }
                i = i + 1
            }
            // check $getSize records dau` tien chi co 1 (user + log_date + net_amt)
            if (continuesUser) {
                var mess = "user_id = " + checkDup(0)(1) + ", log_date = " + checkDup(0)(0) + ", net_amt = " + checkDup(0)(2) + ", times = " + getSize
                Common.logger(mess)
                insertMonitorLog(Constants.ERROR_CODE.PAYMENT_LOG_DUPLICATE, mess, Constants.ERROR_CODE.WARNING)
                notDup = true
            }

            if (!notDup) {
                var mess = "payment log duplicate, sample user_id is: " + checkDup(0)(1)
                Common.logger(mess)
                insertMonitorLog(Constants.ERROR_CODE.PAYMENT_LOG_DUPLICATE, mess, Constants.ERROR_CODE.ERROR)
            }
        } else {
            notDup = true
        }
        var isDup = !notDup
        isDup
    }

    def checkLogDuplicateWarning(data: DataFrame): Boolean = {
        var isDup: Boolean = false
        var n1 = data.select("log_date", "id", "net_amt").distinct().agg(sum("net_amt")).collect.toArray
        var n2 = data.agg(sum("net_amt")).collect.toArray

        var number1 = n1(0)(0)
        var number2 = n2(0)(0)

        if (number1 != number2) {
            isDup = true
            var vv = ((number2.toString.toDouble - number1.toString.toDouble) / number1.toString.toDouble)*100

            var vvv = "%1.2f".format(vv)
            var mess = "payment log duplicate, n1 = " + number1.toString() + ", n2 = " + number2.toString() + " (+" + vvv.toString + "%)"
            Common.logger(mess)

            if(vv >= 40){
                insertMonitorLog(Constants.ERROR_CODE.PAYMENT_LOG_DUPLICATE, mess)
            }
        }
        isDup
    }
}
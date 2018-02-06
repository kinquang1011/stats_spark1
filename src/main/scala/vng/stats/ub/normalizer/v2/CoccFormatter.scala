package vng.stats.ub.normalizer.v2

import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import vng.stats.ub.normalizer.IngameFormatter
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.normalizer.v2.IcaFormatter._
import vng.stats.ub.utils.{Common, Constants, DataUtils}

object CoccFormatter extends IngameFormatter("coccgsn", "coccgsn") {
    def main(args: Array[String]) {
        initParameters(args)
        var conf = new SparkConf().setAppName(gameCode.toUpperCase + " Formatter")
        conf.set("spark.hadoop.varidateOutputSpecs", "false")
        var sc = new SparkContext(conf)
        run(sc)
        sc.stop()
    }

    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {

    }
    override def loginLogoutFormatter(logDate: String, sc: SparkContext): Unit = {
        var loginPath = Common.getInputParquetPath(gameFolder, loginFolder)

        var loginPathList = DataUtils.getMultiFiles(loginPath, logDate, 1)

        def loginFilter(arr: Array[String]): Boolean = {
            arr(0).startsWith(logDate)
            //true
        }

        def loginGenerate(arr: Array[String]): Row = {
            Row(gameCode, arr(0), "login", arr(3), arr(12))
        }

        var sF = Constants.LOGIN_LOGOUT_FIELD
        var loginMapping: Array[String] = Array(sF.GAME_CODE, sF.LOG_DATE, sF.ACTION, sF.ID, sF.LEVEL)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(loginFilter, loginGenerate, loginMapping, loginPathList)
        )

        var formatter = new LoginLogoutFormatter(gameCode, logDate, config, false)
        //formatter.multipleDate = true
        formatter.format(sc)

    }

    override def paymentFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var paymentPath = Common.getInputParquetPath(gameFolder, rechargeFolder)
        var paymentPathList = DataUtils.getMultiFiles(paymentPath, logDate, 1)
        def paymentFilter(arr: Array[String]): Boolean = {
            (arr.length == 12 && arr(3).startsWith(logDate))
            //true
        }

        def paymentGenerate(arr: Array[String]): Row = {
            Row(gameCode, arr(3), arr(4), arr(0), arr(10), arr(11))
        }

        var sF = Constants.PAYMENT_FIELD
        var paymentMapping: Array[String] = Array(sF.GAME_CODE, sF.LOG_DATE, sF.SID, sF.ID, sF.GROSS_AMT, sF.NET_AMT)
        var paymentFormat = new FormatterConfig(paymentFilter, paymentGenerate, paymentMapping, paymentPathList)

        var config: Array[FormatterConfig] = Array(
            paymentFormat
        )
        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        //formatter.multipleDate = true
        formatter.format(sc)
    }

}


package vng.stats.ub.normalizer.v2

import org.apache.spark.{SparkContext, SparkConf}
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.{DateTimeUtils, DataUtils, Common, Constants}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import vng.stats.ub.normalizer.IngameFormatter

object DccFormatter extends IngameFormatter("dcc", "dcc") {
    
    def main(args: Array[String]) {
        
        initParameters(args)

        var conf = new SparkConf().setAppName(gameCode.toUpperCase + " Formatter")
        conf.set("spark.hadoop.varidateOutputSpecs", "false")
        var sc = new SparkContext(conf)
        run(sc)
        sc.stop()
    }

    override def loginLogoutFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var loginPath = Common.getInputParquetPath(gameFolder, "login")
        var loginPathList = DataUtils.getMultiFiles(loginPath, logDate, 1)

        def loginFilter(arr: Array[String]): Boolean = {
            return (arr(6).startsWith(logDate))
        }

        def loginGenerate(arr: Array[String]): Row = {
            Row(gameCode, arr(6), "login", arr(0))
        }

        var sf = Constants.LOGIN_LOGOUT_FIELD
        var loginMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(loginFilter, loginGenerate, loginMapping, loginPathList)
        )

        var formatter = new LoginLogoutFormatter(gameCode, logDate, config, false)
        formatter.format(sc)

    }

    override def paymentFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var paymentPath = Common.getInputParquetPath(gameFolder, "convert_cash")
        var paymentPathList = DataUtils.getMultiFiles(paymentPath, logDate, 1)

        def paymentFilter(arr: Array[String]): Boolean = {
            return (arr(5).startsWith(logDate))
        }

        def paymentGenerate(arr: Array[String]): Row = {
            Row(gameCode, arr(5), arr(0), arr(4), arr(2), arr(2))
        }

        var sf = Constants.PAYMENT_FIELD
        var paymentMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.CHANNEL, sf.GROSS_AMT, sf.NET_AMT)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(paymentFilter, paymentGenerate, paymentMapping, paymentPathList)
        )

        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }

    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var ccuPath = Common.getInputParquetPath(gameFolder, "ccu")
        var ccuPathList = DataUtils.getMultiFiles(ccuPath, logDate, 1)

        def androidFilter(arr: Array[String]): Boolean = {
            return (arr(0).startsWith(logDate))
        }
        
        def iosfilter(arr: Array[String]): Boolean = {
            return (arr(0).startsWith(logDate))
        }

        def androidGenerate(arr: Array[String]): Row = {
            Row(gameCode, arr(0), arr(4), "android", arr(2))
        }
        
        def iosGenerate(arr: Array[String]): Row = {
            Row(gameCode, arr(0), arr(4), "ios", arr(3))
        }

        var sf = Constants.CCU
        var androidMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.SID, sf.OS, sf.CCU)
        var iosMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.SID, sf.OS, sf.CCU)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(androidFilter, androidGenerate, androidMapping, ccuPathList, Map("type" -> "union")),
            new FormatterConfig(iosfilter, iosGenerate, iosMapping, ccuPathList, Map("type" -> "union"))
        )

        var formatter = new CcuFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
}


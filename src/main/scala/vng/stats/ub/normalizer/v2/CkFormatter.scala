package vng.stats.ub.normalizer.v2

import org.apache.spark.{SparkContext, SparkConf}
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.{DateTimeUtils, DataUtils, Common, Constants}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import vng.stats.ub.normalizer.IngameFormatter

object CkFormatter extends IngameFormatter("ck", "ck") {

    def main(args: Array[String]) {
        
        initParameters(args)

        var conf = new SparkConf().setAppName(gameCode.toUpperCase + " Formatter")
        conf.set("spark.hadoop.varidateOutputSpecs", "false")
        var sc = new SparkContext(conf)
        run(sc)
        sc.stop()
    }

    override def loginLogoutFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var loginPath = Common.getInputParquetPath(gameFolder, "login_logout")
        var logoutPath = Common.getInputParquetPath(gameFolder, "login_logout")
        
        var loginPathList = DataUtils.getMultiFiles(loginPath, logDate, 1)
        var logoutPathList = DataUtils.getMultiFiles(logoutPath, logDate, 1)

        def loginFilter(arr: Array[String]): Boolean = {
            return (arr.length == 7 && arr(4).startsWith(logDate))
        }
        
        def logoutFilter(arr: Array[String]): Boolean = {
            return (arr.length == 7 && arr(5).startsWith(logDate))
        }

        def loginGenerate(arr: Array[String]): Row = {
            Row(gameCode, arr(4), "login", arr(6), arr(1), arr(2))
        }
        
        def logoutGenerate(arr: Array[String]): Row = {
            Row(gameCode, arr(5), "logout", arr(6), arr(1), arr(2))
        }

        var sf = Constants.LOGIN_LOGOUT_FIELD
        var loginMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.SID, sf.ID, sf.RID)
        var logoutMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.SID, sf.ID, sf.RID)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(loginFilter, loginGenerate, loginMapping, loginPathList, Map("type" -> "union")),
            new FormatterConfig(logoutFilter, logoutGenerate, logoutMapping, logoutPathList, Map("type" -> "union"))
        )

        var formatter = new LoginLogoutFormatter(gameCode, logDate, config, false)
        formatter.format(sc)

    }
    
    override def paymentFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var paymentPath = Common.getInputParquetPath(gameFolder, "recharge")
        var paymentPathList = DataUtils.getMultiFiles(paymentPath, logDate, 1)

        def paymentFilter(arr: Array[String]): Boolean = {
            return (arr.length == 9 && arr(4).startsWith(logDate))
        }

        def paymentGenerate(arr: Array[String]): Row = {
            Row(gameCode, arr(4), arr(2), arr(1), arr(7), arr(7))
        }

        var sf = Constants.PAYMENT_FIELD
        var paymentMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.SID, sf.ID, sf.GROSS_AMT, sf.NET_AMT)
        
        var paymentFormat = new FormatterConfig(paymentFilter, paymentGenerate, paymentMapping, paymentPathList)
        
        var config: Array[FormatterConfig] = Array(
            paymentFormat
        )

        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
    
    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        var ccuPath = Common.getInputParquetPath(gameFolder, "ccu")
        var ccuPathList = DataUtils.getMultiFiles(ccuPath, logDate, 1)

        def filter(arr: Array[String]): Boolean = {
            return (arr(0).startsWith(logDate))
        }

        def generate(arr: Array[String]): Row = {
            Row(gameCode, arr(0), arr(1), arr(2))
        }

        var sf = Constants.CCU
        var mapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.SID, sf.CCU)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(filter, generate, mapping, ccuPathList)
        )

        var formatter = new CcuFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
}


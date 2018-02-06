package vng.stats.ub.normalizer.v2

import org.apache.spark.sql.Row
import org.apache.spark.{SparkContext, SparkConf}
import vng.stats.ub.normalizer.IngameFormatter
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.{DataUtils, Common, Constants}

import scala.util.matching.Regex

object ContraFormatter extends IngameFormatter("contra", "contra") {

    def main(args: Array[String]) {
        
        initParameters(args)

        var conf = new SparkConf().setAppName(gameCode.toUpperCase + " Formatter")
        conf.set("spark.hadoop.varidateOutputSpecs", "false")
        var sc = new SparkContext(conf)
        run(sc)
        sc.stop()
    }

    override def loginLogoutFormatter(logDate: String, sc: SparkContext): Unit = {
        var loginPath = Common.getInputParquetPath(gameCode, "player_login")
        var logoutPath = Common.getInputParquetPath(gameCode, "player_logout")
        
        var loginPathList = DataUtils.getMultiFiles(loginPath, logDate, 1)
        var logoutPathList = DataUtils.getMultiFiles(logoutPath, logDate, 1)
        
        def loginFilter(arr: Array[String]): Boolean = {
            return (arr.length == 24 && arr(2).startsWith(logDate))
        }

        def logoutFilter(arr: Array[String]): Boolean = {
            return (arr.length == 29 && arr(2).startsWith(logDate))
        }

        def loginGenerate(arr: Array[String]): Row = {

            var osRegex = new Regex("(android|iphone)")
            var os = osRegex.findFirstIn(arr(9).toLowerCase())
            var versionRegex = new Regex("(\\d+(?:\\.\\d+)+)")
            var version = versionRegex.findFirstIn(arr(9))
            Row(gameCode, arr(2), "login", arr(5), arr(1), arr(6), arr(21), "0", arr(10), os, version)
        }
        
        def logoutGenerate(arr: Array[String]): Row = {

            var osRegex = new Regex("(android|iphone)")
            var os = osRegex.findFirstIn(arr(10).toLowerCase())
            var versionRegex = new Regex("(\\d+(?:\\.\\d+)+)")
            var version = versionRegex.findFirstIn(arr(10))
            
            Row(gameCode, arr(2), "logout", arr(5), arr(1), arr(7), arr(22), arr(6), arr(11), os, version)
        }

        var sf = Constants.LOGIN_LOGOUT_FIELD
        var loginMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.SID, sf.LEVEL, sf.DID, sf.ONLINE_TIME, sf.DEVICE, sf.OS, sf.OS_VERSION)
        var logoutMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.SID, sf.LEVEL, sf.DID, sf.ONLINE_TIME, sf.DEVICE, sf.OS, sf.OS_VERSION)
        
        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(loginFilter, loginGenerate, loginMapping, loginPathList, Map("type" -> "union")),
            new FormatterConfig(logoutFilter, logoutGenerate, logoutMapping, logoutPathList, Map("type" -> "union"))
        )

        var formatter = new LoginLogoutFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }

    override def paymentFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var paymentPath = Common.getInputParquetPath(gameCode, "money_flow")
        var paymentPathList = DataUtils.getMultiFiles(paymentPath, logDate, 1)

        def paymentFilter(arr: Array[String]): Boolean = {
            return (
                (arr.length == 15)
                    && (arr(2).startsWith(logDate))
                    && (arr(10) == Constants.CONTRA.PAYMENT_RECHARGE) //recharge
                    && (arr(12) == Constants.CONTRA.PAYMENT_ADD) //add
                )
        }

        def paymentGenerate(arr: Array[String]): Row = {
            
            Row(gameCode, arr(2), arr(5), arr(1), (arr(9).toLong * 200).toString, (arr(9).toLong * 200).toString, arr(7))
        }

        var sf = Constants.PAYMENT_FIELD
        var paymentMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.GROSS_AMT, sf.NET_AMT, sf.LEVEL)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(paymentFilter, paymentGenerate, paymentMapping, paymentPathList)
        )

        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
    
    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var ccuPath = Common.getInputParquetPath(gameCode, "ccu")
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
    
    override def roleRegisterFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var registerPath = Common.getInputParquetPath(gameFolder, "player_register")
        var registerPathList = DataUtils.getMultiFiles(registerPath, logDate, 1)

        def filter(arr: Array[String]): Boolean = {
            return (arr.length == 22 && arr(2).startsWith(logDate))
        }

        def generate(arr: Array[String]): Row = {
            
            var osRegex = new Regex("(android|iphone)")
            var os = osRegex.findFirstIn(arr(7).toLowerCase())
            var versionRegex = new Regex("(\\d+(?:\\.\\d+)+)")
            var version = versionRegex.findFirstIn(arr(7))
            
            Row(gameCode, arr(2), arr(5), arr(1), "", arr(13), "", os, version, arr(8), arr(19))
        }

        var sf = Constants.ROLE_REGISTER_FIELD
        //var mapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.IP, sf.CHANNEL, sf.DEVICE, sf.OS, sf.OS_VERSION, sf.PACKAGE_NAME)
        var mapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.CHANNEL, sf.IP, sf.OS, sf.OS_VERSION, sf.DEVICE, sf.DID)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(filter, generate, mapping, registerPathList)
        )

        var formatter = new RoleRegisterFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
}


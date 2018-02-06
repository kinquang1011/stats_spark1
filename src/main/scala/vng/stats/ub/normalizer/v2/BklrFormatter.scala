package vng.stats.ub.normalizer.v2

import org.apache.spark.{SparkContext, SparkConf}
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.{DateTimeUtils, DataUtils, Common, Constants}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import vng.stats.ub.normalizer.IngameFormatter

object BklrFormatter extends IngameFormatter("bklr", "bklr") {

    def main(args: Array[String]) {
        
        initParameters(args)

        var conf = new SparkConf().setAppName(gameCode.toUpperCase + " Formatter")
        conf.set("spark.hadoop.varidateOutputSpecs", "false")
        var sc = new SparkContext(conf)

        run(sc)
        sc.stop()
    }

    override def loginLogoutFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var loginPath = Common.getInputParquetPath(gameFolder, "player_login")
        var logoutPath = Common.getInputParquetPath(gameFolder, "player_logout")
        
        var loginPathList = DataUtils.getMultiFiles(loginPath, logDate, 1)
        var logoutPathList = DataUtils.getMultiFiles(logoutPath, logDate, 1)

        def loginFilter(arr: Array[String]): Boolean = {
            return (arr.length == 24 && arr(1).startsWith(logDate))
        }

        def logoutFilter(arr: Array[String]): Boolean = {
            return (arr.length == 25 && arr(1).startsWith(logDate))
        }

        def loginGenerate(arr: Array[String]): Row = {
            var os = "ios"
            if(arr(3) == "1"){
                os = "android"
            }
            Row(gameCode, arr(1), "login", arr(4), arr(0), arr(15), arr(5), arr(22), os, arr(8))
        }
        
        def logoutGenerate(arr: Array[String]): Row = {
            var os = "ios"
            if(arr(3) == "1"){
                os = "android"
            }
            Row(gameCode, arr(1), "logout", arr(4), arr(0), arr(16), arr(6), arr(23), os, arr(8))
        }

        var sf = Constants.LOGIN_LOGOUT_FIELD
        var loginMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.SID, sf.CHANNEL, sf.LEVEL, sf.IP, sf.OS, sf.OS_VERSION)
        var logoutMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.SID, sf.CHANNEL, sf.LEVEL, sf.IP, sf.OS, sf.OS_VERSION)
        
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
            return (arr.length == 17 && arr(1).startsWith(logDate))
        }

        def paymentGenerate(arr: Array[String]): Row = {
            var os = "ios"
            if(arr(3) == "1"){
                os = "android"
            }
            Row(gameCode, arr(1), arr(4), arr(0), arr(6), arr(10), arr(10), os, arr(9), arr(15))
        }

        var sf = Constants.PAYMENT_FIELD
        var paymentMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.TRANS_ID, sf.GROSS_AMT, sf.NET_AMT, sf.OS, sf.PACKAGE_NAME, sf.PAY_CHANNEL)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(paymentFilter, paymentGenerate, paymentMapping, paymentPathList)
        )

        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }

    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        
        /*var ccuPath = Common.getInputParquetPath(gameFolder, "ccu")
        var ccuPathList = DataUtils.getMultiFiles(ccuPath, logDate, 1)

        def filter(arr: Array[String]): Boolean = {
            return (arr.length == 5 && arr(1).startsWith(logDate))
        }

        def generate(arr: Array[String]): Row = {
            var os = "ios"
            if(arr(2) == "1"){
                os = "android"
            }
            Row(gameCode, arr(1), arr(0), os, arr(3))
        }

        var sf = Constants.CCU
        var mapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.SID, sf.OS, sf.CCU)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(filter, generate, mapping, ccuPathList)
        )

        var formatter = new CcuFormatter(gameCode, logDate, config, false)
        formatter.format(sc)*/
        
        var ccuPath = Common.getInputParquetPath(gameFolder, "ccu")
        var ccuPathList = DataUtils.getMultiFiles(ccuPath, logDate, 1)

        def filter(arr: Array[String]): Boolean = {
            return (arr.length == 3 && arr(0).startsWith(logDate))
        }

        def generate(arr: Array[String]): Row = {
            var sid = arr(1).replace("BKLR_", "")
            Row(gameCode, arr(0), sid, "", arr(2))
        }

        var sf = Constants.CCU
        var mapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.SID, sf.OS, sf.CCU)

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
            return (arr.length == 23 && arr(1).startsWith(logDate))
        }

        def generate(arr: Array[String]): Row = {
            
            var os = "ios"
            if(arr(3) == "1"){
                os = "android"
            }
            
            Row(gameCode, arr(1), arr(4), arr(0), arr(13), arr(20), os, arr(6), arr(7), arr(19))
        }

        var sf = Constants.ROLE_REGISTER_FIELD
        //var mapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.IP, sf.CHANNEL, sf.DEVICE, sf.OS, sf.OS_VERSION, sf.PACKAGE_NAME)
        var mapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.CHANNEL, sf.IP, sf.OS, sf.OS_VERSION, sf.DEVICE, sf.DID)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(filter, generate, mapping, registerPathList)
        )

        var formatter = new RoleRegisterFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
}


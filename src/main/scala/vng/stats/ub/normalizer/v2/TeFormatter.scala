package vng.stats.ub.normalizer.v2

import org.apache.spark.{SparkContext, SparkConf}
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.{DateTimeUtils, DataUtils, Common, Constants}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import vng.stats.ub.normalizer.IngameFormatter

object TeFormatter extends IngameFormatter("te", "te") {

    def main(args: Array[String]) {
        
        initParameters(args)

        val conf = new SparkConf().setAppName(gameCode.toUpperCase + " Formatter")
        conf.set("spark.hadoop.validateOutputSpecs", "false")
        val sc = new SparkContext(conf)
        run(sc)
        sc.stop()
    }

    override def loginLogoutFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var loginPath = Common.getInputParquetPath(gameFolder, "login")
        var logoutPath = Common.getInputParquetPath(gameFolder, "logout")
        
        var loginPathList = DataUtils.getMultiFiles(loginPath, logDate, 1)
        var logoutPathList = DataUtils.getMultiFiles(logoutPath, logDate, 1)

        def loginFilter(arr: Array[String]): Boolean = {
            return (arr(6).startsWith(logDate))
        }
        
        def logoutFilter(arr: Array[String]): Boolean = {
            return (arr(6).startsWith(logDate))
        }

        def loginGenerate(arr: Array[String]): Row = {
            Row(gameCode, arr(6), "login", arr(7), arr(8), arr(1), arr(11), arr(10))
            /*Row(gameCode, arr(6), "login", arr(7), arr(8), arr(1), "", "")*/
        }
        
        def logoutGenerate(arr: Array[String]): Row = {
            Row(gameCode, arr(6), "logout", arr(7), arr(8), arr(1), arr(11), arr(10))
            /*Row(gameCode, arr(6), "logout", arr(7), arr(8), arr(1), "", "")*/
        }

        var sf = Constants.LOGIN_LOGOUT_FIELD
        var loginMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID, sf.IP, sf.LEVEL)
        var logoutMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID, sf.IP, sf.LEVEL)

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
            return (arr(6).startsWith(logDate) && arr(7) == "1")
        }

        def paymentGenerate(arr: Array[String]): Row = {
            Row(gameCode, arr(6), arr(2), arr(3), arr(4), arr(4), arr(0), arr(1))
        }

        var sf = Constants.PAYMENT_FIELD
        var paymentMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.RID, sf.GROSS_AMT, sf.NET_AMT, sf.SID, sf.TRANS_ID)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(paymentFilter, paymentGenerate, paymentMapping, paymentPathList)
        )

        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
    
    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var ccuPath = Common.getInputParquetPath(gameFolder, "ccu")
        var ccuPathList = DataUtils.getMultiFiles(ccuPath, logDate, 1)

        def filter(arr: Array[String]): Boolean = {
            return (arr(6).startsWith(logDate))
        }

        def generate(arr: Array[String]): Row = {
            Row(gameCode, arr(6), arr(1), arr(8))
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
        
        var rolePath = Common.getInputParquetPath(gameFolder, "create_role")
        var rolePathList = DataUtils.getMultiFiles(rolePath, logDate, 1)

        def filter(arr: Array[String]): Boolean = {
            return (arr(6).startsWith(logDate))
        }

        def generate(arr: Array[String]): Row = {
            Row(gameCode, arr(6), arr(7), arr(1), arr(8), arr(12), arr(13))
        }

        var sf = Constants.ROLE_REGISTER_FIELD
        var mapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.CHANNEL, sf.IP)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(filter, generate, mapping, rolePathList)
        )

        var formatter = new RoleRegisterFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
}


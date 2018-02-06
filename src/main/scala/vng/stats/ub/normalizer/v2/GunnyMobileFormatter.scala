package vng.stats.ub.normalizer.v2

import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import vng.stats.ub.normalizer.IngameFormatter
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.{Common, Constants, DataUtils, DateTimeUtils}

object GunnyMobileFormatter extends IngameFormatter("gnm", "gnm") {
    
    def main(args: Array[String]) {
        var mapParameters: Map[String,String] = Map()
        for(x <- args){
            var xx = x.split("=")
            mapParameters += (xx(0) -> xx(1))
        }



        var conf = new SparkConf().setAppName("SuperfarmMobile Formatter")
        conf.set("spark.hadoop.varidateOutputSpecs", "false")
        var sc = new SparkContext(conf)




        sc.stop()
    }



    override def loginLogoutFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var loginPath = Common.getInputParquetPath("superfarm", "login")
        var logoutPath = Common.getInputParquetPath("superfarm", "logout")
        
        var loginPathList = DataUtils.getMultiFiles(loginPath, logDate, 1)
        var logoutPathList = DataUtils.getMultiFiles(logoutPath, logDate, 1)

        def loginFilter(arr: Array[String]): Boolean = {
            arr.length == 8 && arr(0).startsWith(logDate)
        }
        
        def logoutFilter(arr: Array[String]): Boolean = {
            arr.length == 9 && arr(0).startsWith(logDate)
        }

        def loginGenerate(arr: Array[String]): Row = {
            Row(gameCode, arr(0), "login", arr(4), arr(1), arr(2), arr(5),arr(3), "0")
        }
        
        def logoutGenerate(arr: Array[String]): Row = {
            Row(gameCode, arr(0), "logout", arr(4), arr(1), arr(2), arr(5),arr(3), arr(8))
        }


        var sF = Constants.LOGIN_LOGOUT_FIELD
        var loginMapping: Array[String] = Array(sF.GAME_CODE, sF.LOG_DATE, sF.ACTION, sF.SID, sF.ID, sF.RID, sF.LEVEL, sF.IP, sF.ONLINE_TIME)
        var logoutMapping: Array[String] = Array(sF.GAME_CODE, sF.LOG_DATE, sF.ACTION, sF.SID, sF.ID, sF.RID, sF.LEVEL, sF.IP, sF.ONLINE_TIME)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(loginFilter, loginGenerate, loginMapping, loginPathList, Map("type" -> "union")),
            new FormatterConfig(logoutFilter, logoutGenerate, logoutMapping, logoutPathList, Map("type" -> "union"))
        )

        var formatter = new LoginLogoutFormatter(gameCode, logDate, config, false)
        formatter.format(sc)

    }

    override def paymentFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var paymentPath = Common.getInputParquetPath("superfarm", "recharge")
        var paymentPathList = DataUtils.getMultiFiles(paymentPath, logDate, 1)
        def paymentFilter(arr: Array[String]): Boolean = {
            (arr.length == 14
                && arr(3).startsWith(logDate))
        }

        def paymentGenerate(arr: Array[String]): Row = {
            Row(gameCode, arr(3), arr(0), arr(4), arr(10), arr(11), arr(8), arr(13) )
        }

        var sF = Constants.PAYMENT_FIELD
        var paymentMapping: Array[String] = Array(sF.GAME_CODE, sF.LOG_DATE, sF.ID, sF.SID, sF.GROSS_AMT, sF.NET_AMT, sF.CHANNEL, sF.TRANS_ID)

        var paymentFormat = new FormatterConfig(paymentFilter, paymentGenerate, paymentMapping, paymentPathList)

        var config: Array[FormatterConfig] = Array(
            paymentFormat
        )
        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }

}


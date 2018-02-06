package vng.stats.ub.normalizer.v2

import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import vng.stats.ub.normalizer.IngameFormatter
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.{Common, Constants}
import vng.stats.ub.utils.DateTimeUtils

object JxmFormatter extends IngameFormatter ("jxm", "jxm") {

    val MONEY_FLOW_NGUYENBAO_TYPE = "7"

    def main(args: Array[String]) {
        initParameters(args)
        var conf = new SparkConf().setAppName(gameCode.toUpperCase + " Formatter")
        conf.set("spark.hadoop.varidateOutputSpecs", "false")
        var sc = new SparkContext(conf)
        if (!mapParameters.contains("hourly")){
            addFunction(Constants.ROLE_REGISTER_TYPE, true)
            addFunction(Constants.MONEY_FLOW_TYPE)
            addFunction(Constants.CHARACTER_INFO_TYPE)
        }
        if (mapParameters.contains("hourly")){
            delFunction(Constants.CCU_TYPE)
        }
        run(sc)
        sc.stop()
    }

    override def characterInfoFormatter(logDate: String, sc: SparkContext): Unit = {
        /*var formatter = new CharacterInfoFormatter(gameCode, logDate, null, false)
        formatter.setOutputPath(getOutputPath(logDate, Constants.PARQUET_2.CHARACTER_INFO_OUTPUT_FOLDER))
        formatter.format(sc)*/
    }

    override def moneyFlowFormatter(logDate: String, sc: SparkContext): Unit = {
        /*var pathList = getInputPath("MoneyFlow", logDate, 0)
        def filter(arr: Array[String]): Boolean = {
            //arr.length == 16 && arr(2).startsWith(logDate)
            arr(15) == MONEY_FLOW_NGUYENBAO_TYPE
        }

        def generate(arr: Array[String]): Row = {
            Row(gameCode, arr(2), arr(6), arr(5), arr(7), arr(11), arr(10), arr(15), arr(12), arr(13), arr(14), arr(8))
        }

        var sf = Constants.MONEY_FLOW_FIELD
        var mapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID,
            sf.I_MONEY, sf.MONEY_AFTER, sf.MONEY_TYPE, sf.REASON, sf.SUB_REASON, sf.ADD_OR_REDUCE, sf.LEVEL)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(filter, generate, mapping, pathList)
        )

        var formatter = new MoneyFlowFormatter(gameCode, logDate, config, false)
        formatter.setOutputPath(getOutputPath(logDate, Constants.PARQUET_2.MONEY_FLOW_OUTPUT_FOLDER))
        formatter.format(sc)*/
    }

    override def loginLogoutFormatter(logDate: String, sc: SparkContext): Unit = {
        var loginPathList = getInputPath("PlayerLogin", logDate, 1)
        var logoutPathList = getInputPath("PlayerLogout", logDate, 1)

        def loginFilter(arr: Array[String]): Boolean = {
            arr(2).startsWith(logDate) && arr.length >= 24
        }

        def logoutFilter(arr: Array[String]): Boolean = {
            arr(2).startsWith(logDate) && arr.length >= 26
        }

        def loginGenerate(arr: Array[String]): Row = {
            var platform = "other"
            if (arr(4) == "0") {
                platform = "ios"
            } else if (arr(4) == "1") {
                platform = "android"
            }
            Row(gameCode, arr(2), "login", arr(6).trim(), arr(7), arr(5), "0", arr(8), platform, arr(23), arr(18), arr(12), "")
        }

        def logoutGenerate(arr: Array[String]): Row = {
            var platform = "other"
            if (arr(4) == "0") {
                platform = "ios"
            } else if (arr(4) == "1") {
                platform = "android"
            }
            Row(gameCode, arr(2), "logout", arr(6).trim(), arr(7), arr(5), arr(8), arr(9), platform, arr(24), arr(19), arr(13), arr(25))
        }

        var sf = Constants.LOGIN_LOGOUT_FIELD
        var loginMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID, sf.ONLINE_TIME, sf.LEVEL, sf.OS, sf.DID, sf.CHANNEL, sf.DEVICE, sf.IP)
        var logoutMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID, sf.ONLINE_TIME, sf.LEVEL, sf.OS, sf.DID, sf.CHANNEL, sf.DEVICE, sf.IP)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(loginFilter, loginGenerate, loginMapping, loginPathList, Map("type" -> "union")),
            new FormatterConfig(logoutFilter, logoutGenerate, logoutMapping, logoutPathList, Map("type" -> "union"))
        )

        var formatter = new LoginLogoutFormatter(gameCode, logDate, config, false)
        formatter.setOutputPath(getOutputPath(logDate, Constants.PARQUET_2.LOGIN_LOGOUT_OUTPUT_FOLDER))
        formatter.format(sc)
    }

    override def paymentFormatter(logDate: String, sc: SparkContext): Unit = {
        var paymentPathList = getInputPath("RechargeFlow", logDate, 1)
        def paymentFilter(arr: Array[String]): Boolean = {
            //arr.length == 12 && arr(2).startsWith(logDate)
          arr(2).startsWith(logDate)
        }

        def paymentGenerate(arr: Array[String]): Row = {
            var platform = "other"
            if (arr(4) == "0") {
                platform = "ios"
            } else if (arr(4) == "1") {
                platform = "android"
            }
            var level ="0";
            if(arr.length==12){
              level =arr(11)
            }
            Row(gameCode, arr(2), arr(6).trim(), arr(5), arr(7), (arr(9).toDouble * 100).toString(), (arr(9).toDouble * 100).toString(), level, platform, arr(8))
        }

        var sf = Constants.PAYMENT_FIELD
        var paymentMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.GROSS_AMT, sf.NET_AMT, sf.LEVEL, sf.OS, sf.IP)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(paymentFilter, paymentGenerate, paymentMapping, paymentPathList)
        )

        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.setOutputPath(getOutputPath(logDate, Constants.PARQUET_2.PAYMENT_OUTPUT_FOLDER))
        formatter.format(sc)
    }

    /*override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        var ccuPathList = getInputPath("ccu", logDate, 1)

        def filter(arr: Array[String]): Boolean = {
            return (arr.length == 3 && arr(0).startsWith(logDate))
        }

        def generate(arr: Array[String]): Row = {
            
            val formatTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            var logDate = arr(0)
            
            var min = logDate.substring(14,16).toInt
            min = min - (min % 5)
            
            var newDate = DateTimeUtils.formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH", logDate) + ":%02d:00".format(min)
            
            Row(gameCode, newDate, arr(1), arr(2))
        }

        var sf = Constants.CCU
        var mapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.SID, sf.CCU)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(filter, generate, mapping, ccuPathList)
        )

        var formatter = new CcuFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }*/
}
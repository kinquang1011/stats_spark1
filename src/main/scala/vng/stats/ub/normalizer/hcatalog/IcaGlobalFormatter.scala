package vng.stats.ub.normalizer.hcatalog

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import vng.stats.ub.normalizer.IngameFormatter
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2.CcuFormatter
import vng.stats.ub.normalizer.format.v2.HiveQueryConfig
import vng.stats.ub.normalizer.format.v2.LoginLogoutFormatter
import vng.stats.ub.normalizer.format.v2.PaymentFormatter
import vng.stats.ub.utils.Constants
import vng.stats.ub.utils.DateTimeUtils
import vng.stats.ub.utils.Common

object IcaGlobalFormatter extends IngameFormatter("ftgfbs2", "ftgfbs2") {

    def main(args: Array[String]) {
        
        initParameters(args)

        var conf = new SparkConf().setAppName(gameCode.toUpperCase + " Formatter")
        conf.set("spark.hadoop.varidateOutputSpecs", "false")
        var sc = new SparkContext(conf)
        run(sc)
        sc.stop()
    }
    
    override def getPartition(logDate: String, numOfDaily: Int = 2, numOfHourly: Int = 1): Array[String] = {
        
        var partition: Array[String] = super.getPartition(logDate, numOfDaily, numOfHourly)
        
        if(!mapParameters.contains("hourly")){
            
            // get hourly partition
            var dateFormat = HOURLY_DS_FORMAT
            
            var nextDay = DateTimeUtils.getDateDifferent(1, logDate, Constants.TIMING, Constants.A1)
            partition = partition ++ Array(DateTimeUtils.formatDate(Constants.DEFAULT_DATE_FORMAT, dateFormat, nextDay))
        }
        Common.logger(partition.toString())
        partition
    }
    
    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        
        val sf = Constants.CCU
        var partition = getPartition(logDate)
        var rechargeQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("log_date", sf.LOG_DATE)
            select("server_id", sf.SID)
            select("ccu", sf.CCU)

            from(gameCode + ".ccu")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
        }
        var ccuConfig = new FormatterConfig(null, null, null, null)
        ccuConfig.hiveQueryConfig = rechargeQuery
        ccuConfig.inputFileType = Constants.INPUT_FILE_TYPE.HIVE

        var config: Array[FormatterConfig] = Array(
            ccuConfig
        )
        
        var formatter = new CcuFormatter(gameCode, logDate, config, false)
        formatter.setOutputPath(getOutputPath(logDate, Constants.PARQUET_2.CCU_OUTPUT_FOLDER))
        formatter.format(sc)
    }
    
    override def loginLogoutFormatter(logDate: String, sc: SparkContext): Unit = {
        val sf = Constants.LOGIN_LOGOUT_FIELD
        var partition = getPartition(logDate)
        var loginQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("login_date", sf.LOG_DATE)
            select("'login'", sf.ACTION)
            select("user_id", sf.ID)
            select("role_id", sf.RID)
            select("server_id", sf.SID)
            select("level", sf.LEVEL)
            select("os_platform", sf.OS)
            select("os_version", sf.OS_VERSION)
            select("device_name", sf.DEVICE)
            select("device_id", sf.DID)
            select("'0'", sf.ONLINE_TIME)
            from(gameCode + ".login")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where("result", "1")
            where_in("ds", partition)
        }
        var logoutQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("logout_date", sf.LOG_DATE)
            select("'logout'", sf.ACTION)
            select("user_id", sf.ID)
            select("role_id", sf.RID)
            select("server_id", sf.SID)
            select("level", sf.LEVEL)
            select("os_platform", sf.OS)
            select("os_version", sf.OS_VERSION)
            select("device_name", sf.DEVICE)
            select("device_id", sf.DID)
            select("total_online_second", sf.ONLINE_TIME)
            from(gameCode + ".logout")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where("result", "1")
            where_in("ds", partition)
        }

        var loginConfig = new FormatterConfig(null, null, null, null)
        loginConfig.hiveQueryConfig = loginQuery
        loginConfig.inputFileType = Constants.INPUT_FILE_TYPE.HIVE

        var logoutConfig = new FormatterConfig(null, null, null, null)
        logoutConfig.hiveQueryConfig = logoutQuery
        logoutConfig.inputFileType = Constants.INPUT_FILE_TYPE.HIVE

        var config: Array[FormatterConfig] = Array(
            loginConfig,
            logoutConfig
        )
        var formatter = new LoginLogoutFormatter(gameCode, logDate, config, false)
        formatter.setOutputPath(getOutputPath(logDate, Constants.PARQUET_2.LOGIN_LOGOUT_OUTPUT_FOLDER))
        formatter.format(sc)
    }

    override def paymentFormatter(logDate: String, sc: SparkContext): Unit = {
        val sf = Constants.PAYMENT_FIELD
        var partition = getPartition(logDate)
        var rechargeQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("log_date", sf.LOG_DATE)
            select("user_id", sf.ID)
            select("role_id", sf.RID)
            select("server_id", sf.SID)
            select("payment_gateway", sf.PAY_CHANNEL)
            select("gross_revenue", sf.GROSS_AMT)
            select("net_revenue", sf.NET_AMT)
            select("level", sf.LEVEL)
            select("transaction_id", sf.TRANS_ID)
            select("ip_paying", sf.IP)
            select("device_id", sf.DID)
            from(gameCode + ".recharge")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where("result", "1")
            where_in("ds", partition)
        }
        var rechargeConfig = new FormatterConfig(null, null, null, null)
        rechargeConfig.hiveQueryConfig = rechargeQuery
        rechargeConfig.inputFileType = Constants.INPUT_FILE_TYPE.HIVE

        var config: Array[FormatterConfig] = Array(
            rechargeConfig
        )
        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.setOutputPath(getOutputPath(logDate, Constants.PARQUET_2.PAYMENT_OUTPUT_FOLDER))
        formatter.format(sc)
    }
}


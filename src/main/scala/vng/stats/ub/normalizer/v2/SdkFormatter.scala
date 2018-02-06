package vng.stats.ub.normalizer.v2

import org.apache.spark.{SparkConf, SparkContext}
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.{Common, Constants, DataUtils, DateTimeUtils}

/**
 * Created by tuonglv on 17/05/2016.
 */
object SdkFormatter {
    var inputPath = ""
    var outputFolder = ""
    //val before2016List = List("contra", "dttk", "pmcl", "tlbbm", "tvl", "wefight")
    val before2016List = List("aaaaaaa")
    var extraTime = 0L
    var changeRate =1d
    def main(args: Array[String]) {
        var mapParameters: Map[String, String] = Map()
        for (x <- args) {
            val xx = x.split("=")
            mapParameters += (xx(0) -> xx(1))
        }
        val gameCode = mapParameters("gameCode").toString.toLowerCase
        inputPath = mapParameters("inputPath")
        outputFolder = mapParameters("outputFolder")

        var hasNew = true
        if (before2016List.contains(gameCode)) {
            hasNew = false
        }
        if (mapParameters.contains("extraTime")) {
            extraTime = mapParameters("extraTime").toLong
        }
        if (mapParameters.contains("changeRate")) {
            val rateStr:String = mapParameters("changeRate")
            if(!rateStr.equals("0")){
                changeRate = rateStr.toDouble
            }

        }
        val conf = new SparkConf().setAppName("SDK Formatter::" + gameCode.toUpperCase)
        conf.set("spark.hadoop.validateOutputSpecs", "false")
        val sc = new SparkContext(conf)

        if (mapParameters.contains("rerun") && mapParameters("rerun").toInt == 1) {
            var startDate = mapParameters("startDate")
            var endDate = mapParameters("endDate")
            while (startDate != endDate) {
                if (mapParameters.contains("logType")) {
                    val logType = mapParameters("logType")
                    logType match {
                        case Constants.LOGIN_LOGOUT_TYPE =>
                            loginFormatter(startDate, gameCode, sc)
                        case Constants.PAYMENT_TYPE =>
                            paymentFormatter(startDate, gameCode, sc)
                        case Constants.ACC_REGISTER_TYPE =>
                            accRegisterFormatter(startDate, gameCode, sc)
                        case Constants.FIRST_CHARGE_TYPE =>
                            firstChargeFormatter(startDate, gameCode, sc)
                        case Constants.COUNTRY_MAPPING_TYPE =>
                            countryMapping(startDate, gameCode, sc)

                    }
                } else {
                    loginFormatter(startDate, gameCode, sc)
                    //paymentFormatter(startDate, gameCode, sc)
                    if (hasNew) {
                        accRegisterFormatter(startDate, gameCode, sc)
                        //firstChargeFormatter(startDate, gameCode, sc)
                    }
                    runOtherFunctionsByGame(startDate, gameCode, sc)
                }
                startDate = DateTimeUtils.getDateDifferent(1, startDate, Constants.TIMING, Constants.A1)
            }
        } else {
            val logDate = mapParameters("logDate")
            if (mapParameters.contains("logType")) {
                val logType = mapParameters("logType")
                logType match {
                    case Constants.LOGIN_LOGOUT_TYPE =>
                        loginFormatter(logDate, gameCode, sc)
                    case Constants.PAYMENT_TYPE =>
                        paymentFormatter(logDate, gameCode, sc)
                    case Constants.ACC_REGISTER_TYPE =>
                        accRegisterFormatter(logDate, gameCode, sc)
                    case Constants.FIRST_CHARGE_TYPE =>
                        firstChargeFormatter(logDate, gameCode, sc)
                    case Constants.COUNTRY_MAPPING_TYPE =>
                        countryMapping(logDate, gameCode, sc)
                    case Constants.NEW_LOGIN_DEVICE_TYPE =>
                        deviceRegisterFormatter(logDate, gameCode, sc)
                    case Constants.NEW_PAID_DEVICE_TYPE =>
                        deviceFirstChargeFormatter(logDate, gameCode, sc)
                }
            } else {
                loginFormatter(logDate, gameCode, sc)
                paymentFormatter(logDate, gameCode, sc)
                if (hasNew) {
                    accRegisterFormatter(logDate, gameCode, sc)
                    firstChargeFormatter(logDate, gameCode, sc)
                }
                runOtherFunctionsByGame(logDate, gameCode, sc)
            }
        }
        sc.stop()
    }

    def runOtherFunctionsByGame(logDate: String, gameCode: String, sc: SparkContext): Unit = {
        gameCode match {
            case "stct" =>
                countryMapping(logDate, gameCode, sc)
                //deviceFirstChargeFormatter(logDate, gameCode, sc)
                deviceRegisterFormatter(logDate, gameCode, sc)
            case _ =>
        }
    }
    def deviceRegisterFormatter(logDate: String, gameCode: String, sc: SparkContext): Unit = {
        Common.logger("deviceRegisterFormatter start")
        var dateBeforeOneDay = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
        var path1 = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.TOTAL_DEVICE_LOGIN_OUTPUT_FOLDER, dateBeforeOneDay, true)
        var path2 = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.LOGIN_LOGOUT_OUTPUT_FOLDER, logDate, true)
        var pathList1 = path1
        var pathList2 = path2

        var sF = Constants.DEVICE_REGISTER_FIELD

        var on: Array[String] = Array(sF.DID)
        var where: Map[String, Map[String, Any]] = Map("A" -> Map(sF.DID -> "is null"))
        var fields: Map[String, Array[String]] = Map("B" -> Array(sF.GAME_CODE, sF.LOG_DATE, sF.PACKAGE_NAME, sF.CHANNEL, sF.ID, sF.DID, sF.SID, sF.IP, sF.DEVICE, sF.OS, sF.OS_VERSION))

        var format1: FormatterConfig = new FormatterConfig(null, null, null, pathList1)
        format1.setInputFileType("parquet")
        var format2: FormatterConfig = new FormatterConfig(null, null, null, pathList2, Map("type" -> "right join", "fields" -> fields, "where" -> where, "on" -> on))
        format2.setInputFileType("parquet")

        var config: Array[FormatterConfig] = Array(
            format1,
            format2
        )
        var formatter = new NewDeviceFromLogin(gameCode, logDate, config)
        formatter.format(sc)
        Common.logger("deviceRegisterFormatter end")
    }

    def accRegisterFormatter(logDate: String, gameCode: String, sc: SparkContext): Unit = {
        Common.logger("accRegisterFormatter start")
        var dateBeforeOneDay = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
        var path1 = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.TOTAL_ACC_LOGIN_OUTPUT_FOLDER, dateBeforeOneDay, true)
        var path2 = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.LOGIN_LOGOUT_OUTPUT_FOLDER, logDate, true)
        var pathList1 = path1
        var pathList2 = path2

        var sF = Constants.ACC_REGISTER_FIELD

        var on: Array[String] = Array(sF.ID)
        var where: Map[String, Map[String, Any]] = Map("A" -> Map(sF.ID -> "is null"))
        var fields: Map[String, Array[String]] = Map("B" -> Array(sF.GAME_CODE, sF.LOG_DATE, sF.PACKAGE_NAME, sF.CHANNEL, sF.ID, sF.SID, sF.IP, sF.DEVICE, sF.OS, sF.OS_VERSION))

        var format1: FormatterConfig = new FormatterConfig(null, null, null, pathList1)
        format1.setInputFileType("parquet")
        var format2: FormatterConfig = new FormatterConfig(null, null, null, pathList2, Map("type" -> "right join", "fields" -> fields, "where" -> where, "on" -> on))
        format2.setInputFileType("parquet")

        var config: Array[FormatterConfig] = Array(
            format1,
            format2
        )
        var formatter = new NewAccFromLogin(gameCode, logDate, config)
        formatter.format(sc)
        Common.logger("accRegisterFormatter end")
    }

    def deviceFirstChargeFormatter(logDate: String, gameCode: String, sc: SparkContext): Unit = {
        Common.logger("deviceFirstChargeFormatter start")
        var dateBeforeOneDay = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
        var path1 = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.TOTAL_DEVICE_PAID_OUTPUT_FOLDER, dateBeforeOneDay, true)
        var path2 = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.PAYMENT_OUTPUT_FOLDER, logDate, true)
        var pathList1 = path1
        var pathList2 = path2

        var sF = Constants.DEVICE_FIRSTCHARGE_FIELD

        var on: Array[String] = Array(sF.DID)
        var where: Map[String, Map[String, Any]] = Map("A" -> Map(sF.DID -> "is null"))
        var fields: Map[String, Array[String]] = Map("B" -> Array(sF.GAME_CODE, sF.LOG_DATE, sF.PACKAGE_NAME, sF.CHANNEL, sF.PAY_CHANNEL, sF.DID, sF.ID, sF.DEVICE, sF.OS, sF.OS_VERSION))

        var format1: FormatterConfig = new FormatterConfig(null, null, null, pathList1)
        format1.setInputFileType("parquet")
        var format2: FormatterConfig = new FormatterConfig(null, null, null, pathList2, Map("type" -> "right join", "fields" -> fields, "where" -> where, "on" -> on))
        format2.setInputFileType("parquet")

        var config: Array[FormatterConfig] = Array(
            format1,
            format2
        )
        var formatter = new DeviceFirstChargeFromPaying(gameCode, logDate, config)
        formatter.format(sc)
        Common.logger("deviceFirstChargeFormatter end")
    }

    def firstChargeFormatter(logDate: String, gameCode: String, sc: SparkContext): Unit = {
        Common.logger("firstChargeFormatter start")
        var dateBeforeOneDay = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
        var path1 = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.TOTAL_ACC_PAID_OUTPUT_FOLDER, dateBeforeOneDay, true)
        var path2 = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.PAYMENT_OUTPUT_FOLDER, logDate, true)
        var pathList1 = path1
        var pathList2 = path2

        var sF = Constants.FIRSTCHARGE_FIELD

        var on: Array[String] = Array(sF.ID)
        var where: Map[String, Map[String, Any]] = Map("A" -> Map(sF.ID -> "is null"))
        var fields: Map[String, Array[String]] = Map("B" -> Array(sF.GAME_CODE, sF.LOG_DATE, sF.PACKAGE_NAME, sF.CHANNEL, sF.PAY_CHANNEL, sF.ID, sF.DEVICE, sF.OS, sF.OS_VERSION))

        var format1: FormatterConfig = new FormatterConfig(null, null, null, pathList1)
        format1.setInputFileType("parquet")
        var format2: FormatterConfig = new FormatterConfig(null, null, null, pathList2, Map("type" -> "right join", "fields" -> fields, "where" -> where, "on" -> on))
        format2.setInputFileType("parquet")

        var config: Array[FormatterConfig] = Array(
            format1,
            format2
        )
        var formatter = new FirstChargeFromPaying(gameCode, logDate, config)
        formatter.format(sc)
        Common.logger("firstChargeFormatter end")
    }

    def loginFormatter(logDate: String, gameCode: String, sc: SparkContext): Unit = {
        Common.logger("loginFormatter start")
        val path1 = Common.geInputSdkRawLogPath(inputPath)
        val fileName = Common.getGameCodeInFileName(gameCode, "loginlogout").toUpperCase + "_Login_InfoLog"
        val pathList1 = DataUtils.getMultiSdkLogFiles(path1, logDate, fileName, 1)
        var formmatterConfig = new FormatterConfig(null, null, null, pathList1)
        formmatterConfig.setExtraTime(extraTime)
        val config: Array[FormatterConfig] = Array(
            formmatterConfig
        )

        val formatter = new LoginLogoutJsonFormatter(gameCode, logDate, config)
        formatter.format(sc)
        Common.logger("loginFormatter end")
    }

    def countryMapping(logDate: String, gameCode: String, sc: SparkContext): Unit = {
        val path = Common.geInputSdkRawLogPath(inputPath)

        val fileName1 = Common.getGameCodeInFileName(gameCode, "loginlogout").toUpperCase + "_Login_InfoLog"
        val pathList1 = DataUtils.getMultiSdkLogFiles(path, logDate, fileName1, 5)

        var fileName2 = "Log_" + Common.getGameCodeInFileName(gameCode, "payment").toUpperCase + "_DBGAdd"
        val pathList2 = DataUtils.getMultiSdkLogFiles(path, logDate, fileName2, 1)

        var config1 = new FormatterConfig(null, null, null, pathList1)
        var config2 = new FormatterConfig(null, null, null, pathList2)
        config1.setExtraTime(extraTime)
        config1.setConfigName(Constants.LOGIN_LOGOUT_TYPE)
        config2.setExtraTime(extraTime)
        config2.setConfigName(Constants.PAYMENT_TYPE)
        val config: Array[FormatterConfig] = Array(
            config1
        //    config2
        )
        val formatter = new CountryMappingJsonFormatter(gameCode, logDate, config)
        formatter.format(sc)
    }

    def paymentFormatter(logDate: String, gameCode: String, sc: SparkContext): Unit = {
        Common.logger("paymentFormatter start")
        val path = Common.geInputSdkRawLogPath(inputPath)
        var fileName1 = "Log_" + Common.getGameCodeInFileName(gameCode, "payment").toUpperCase + "_DBGAdd"
        val pathList1 = DataUtils.getMultiSdkLogFiles(path, logDate, fileName1, 1)

        val fileName2 = Common.getGameCodeInFileName(gameCode, "loginlogout").toUpperCase + "_Login_InfoLog"
        val pathList2 = DataUtils.getMultiSdkLogFiles(path, logDate, fileName2, 1)

        Common.logger(pathList1)
        Common.logger(pathList2)

        var formmatterConfig1 = new FormatterConfig(null, null, null, pathList1)
        var formmatterConfig2 = new FormatterConfig(null, null, null, pathList2)
        formmatterConfig1.setExtraTime(extraTime)
        formmatterConfig2.setExtraTime(extraTime)
        formmatterConfig1.setConvertRate(changeRate)
        val config: Array[FormatterConfig] = Array(
            formmatterConfig1,
            formmatterConfig2
        )

        val formatter = new PaymentJsonFormatter(gameCode, logDate, config)
        formatter.format(sc)
        Common.logger("paymentFormatter end")
    }
}
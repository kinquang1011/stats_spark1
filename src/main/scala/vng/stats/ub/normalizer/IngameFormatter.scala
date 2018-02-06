package vng.stats.ub.normalizer

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row}
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.{ Common, Constants, DateTimeUtils}
/**
 * Created by tuonglv on 12/09/2016.
 */
class IngameFormatter(val gameCode: String, val gameFolder: String) extends Serializable {

    val INGAME_WAREHOUSE = "/ge/warehouse"
    val DRAGON_WAREHOUSE = "/ge/dragon/warehouse"
    val DAILY_DS_FORMAT = "yyyy-MM-dd"
    val HOURLY_DS_FORMAT = "yyyyMMdd"

    var ccuFolder = "ccu"
    var loginFolder = "login"
    var logoutFolder = "logout"
    var rechargeFolder = "recharge"

    var mapParameters: Map[String, String] = Map()
    type MyFunction = (String, SparkContext) => Unit
    private var runFormats: Array[String] = Array()

    def initParameters(args: Array[String]): Unit = {
        for (x <- args) {
            var xx = x.split("=")
            mapParameters += (xx(0) -> xx(1))
        }
        registerFunctions()
    }

    def clearFunction(): Unit = {
        runFormats = Array()
    }

    def registerFunctions(): Unit = {
        if (mapParameters.contains("logType")) {
            val logType = mapParameters("logType")
            addFunction(logType, true)
        } else {
            addFunction(Constants.LOGIN_LOGOUT_TYPE, true)
            addFunction(Constants.PAYMENT_TYPE, true)
            addFunction(Constants.ACC_REGISTER_TYPE, true)
            addFunction(Constants.FIRST_CHARGE_TYPE, true)
            addFunction(Constants.CCU_TYPE, true)
            //addFunction(Constants.ROLE_REGISTER_TYPE, true)
            if(!mapParameters.contains("hourly")){
                addFunction(Constants.TOTAL_LOGIN_ACC_TYPE, true)
                addFunction(Constants.TOTAL_PAID_ACC_TYPE, true)
            }
        }
    }

    def addFunction(functionName: String, force: Boolean = false): Unit = {
        if (force || !mapParameters.contains("logType")) {
            runFormats = runFormats ++ Array(functionName)
            Common.logger("Function " + functionName + " was added")
        }
    }
    def delFunction(functionName: String): Unit = {
        var tmp = runFormats.toBuffer
        tmp  = tmp -- Array(functionName)
        Common.logger("Function " + functionName + " was removed")
        runFormats = tmp.toArray
    }

    def run(sc: SparkContext): Unit = {
        if (mapParameters.contains("rerun") && mapParameters("rerun").toInt == 1) {
            var startDate = mapParameters("startDate")
            var endDate = mapParameters("endDate")
            while (startDate != endDate) {
                runFormats.foreach { functionName =>
                    _run(functionName, startDate, sc)
                }
                createDoneFlag(startDate, sc)
                startDate = DateTimeUtils.getDateDifferent(1, startDate, Constants.TIMING, Constants.A1)
            }
        } else {
            val logDate = mapParameters("logDate")
            runFormats.foreach { functionName =>
                _run(functionName, logDate, sc)
            }
            createDoneFlag(logDate, sc)
        }
    }

    private def _run(functionName:String, logDate:String, sc: SparkContext): Unit = {
        Common.logger("Function " + functionName + " started")
        functionName match {
            case Constants.LOGIN_LOGOUT_TYPE =>
                loginLogoutFormatter(logDate, sc)
            case Constants.PAYMENT_TYPE =>
                paymentFormatter(logDate, sc)
            case Constants.ACC_REGISTER_TYPE =>
                accRegisterFormatter(logDate, sc)
            case Constants.FIRST_CHARGE_TYPE =>
                firstChargeFormatter(logDate, sc)
            case Constants.CCU_TYPE =>
                ccuFormatter(logDate, sc)
            case Constants.ROLE_REGISTER_TYPE =>
                roleRegisterFormatter(logDate, sc)
            case Constants.SERVER_ACC_REGISTER_TYPE =>
                serverAccRegisterFormatter(logDate, sc)
            case Constants.MONEY_FLOW_TYPE =>
                moneyFlowFormatter(logDate, sc)
            case Constants.CHARACTER_INFO_TYPE =>
                characterInfoFormatter(logDate, sc)
            case Constants.TOTAL_LOGIN_ACC_TYPE =>
                totalAccLoginFormatter(logDate, sc)
            case Constants.TOTAL_PAID_ACC_TYPE =>
                totalAccPaidFormatter(logDate, sc)
            case Constants.NEW_LOGIN_DEVICE_TYPE =>
                deviceRegisterFormatter(logDate, sc)
            case Constants.NEW_PAID_DEVICE_TYPE =>
                deviceFirstChargeFormatter(logDate, sc)
            case default =>
                Common.logger("Function name = " + functionName + " not found")
        }
        Common.logger("Function " + functionName + " end")
    }

    def loginLogoutFormatter(logDate: String, sc: SparkContext): Unit = {
    }

    def serverAccRegisterFormatter(logDate: String, sc: SparkContext): Unit = {}

    def moneyFlowFormatter(logDate: String, sc: SparkContext): Unit = {}

    def characterInfoFormatter(logDate: String, sc: SparkContext): Unit = {}

    def paymentFormatter(logDate: String, sc: SparkContext): Unit = {}

    def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        var ccuPathList = getInputPath(ccuFolder, logDate, 1)
        def filter(arr: Array[String]): Boolean = {
            arr(0).startsWith(logDate)
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
        formatter.setOutputPath(getOutputPath(logDate, Constants.PARQUET_2.CCU_OUTPUT_FOLDER))
        formatter.format(sc)
    }

    def accRegisterFormatter(logDate: String, sc: SparkContext): Unit = {
        var dateBeforeOneDay = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
        var pathList1 = getOutputPath(dateBeforeOneDay, Constants.PARQUET_2.TOTAL_ACC_LOGIN_OUTPUT_FOLDER, "data")
        var pathList2 = getOutputPath(logDate, Constants.PARQUET_2.LOGIN_LOGOUT_OUTPUT_FOLDER)

        var sF = Constants.ACC_REGISTER_FIELD

        var on: Array[String] = Array(sF.ID)
        var where: Map[String, Map[String, Any]] = Map("A" -> Map(sF.ID -> "is null"))
        var fields: Map[String, Array[String]] = Map("B" -> Array(sF.GAME_CODE, sF.LOG_DATE, sF.PACKAGE_NAME, sF.CHANNEL, sF.ID, sF.SID, sF.IP, sF.DEVICE, sF.OS, sF.OS_VERSION))

        var format1: FormatterConfig = new FormatterConfig(null, null, null, pathList1)
        format1.setInputFileType(Constants.INPUT_FILE_TYPE.PARQUET)
        var format2: FormatterConfig = new FormatterConfig(null, null, null, pathList2, Map("type" -> "right join", "fields" -> fields, "where" -> where, "on" -> on))
        format2.setInputFileType(Constants.INPUT_FILE_TYPE.PARQUET)
        def customGenerate(dt:DataFrame): DataFrame = {
            dt.sort(dt("id"), dt("os").desc, dt("log_date").asc).dropDuplicates(Seq("id"))
        }
        format2.setCustomGenerate(customGenerate)

        var config: Array[FormatterConfig] = Array(
            format1,
            format2
        )
        var formatter = new NewAccFromLogin(gameCode, logDate, config, false)
        formatter.setOutputPath(getOutputPath(logDate, Constants.PARQUET_2.ACC_REGISTER_OUTPUT_FOLDER))
        formatter.format(sc)
    }

    def totalAccLoginFormatter(logDate: String, sc: SparkContext): Unit = {
        var dateBeforeOneDay = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
        var pathList1 = getOutputPath(dateBeforeOneDay, Constants.PARQUET_2.TOTAL_ACC_LOGIN_OUTPUT_FOLDER, "data")
        var pathList2 = getOutputPath(logDate, Constants.PARQUET_2.ACC_REGISTER_OUTPUT_FOLDER)

        def accregisterGenerate(df:DataFrame): DataFrame = {
            df.select("game_code", "log_date", "id")
        }
        var sf = Constants.TOTAL_ACC_LOGIN
        var mapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID)

        var format1: FormatterConfig = new FormatterConfig(null, null, mapping, pathList1)
        format1.setInputFileType(Constants.INPUT_FILE_TYPE.PARQUET)
        var format2: FormatterConfig = new FormatterConfig(null, null, null, pathList2, Map("type" -> "union"))
        format2.setInputFileType(Constants.INPUT_FILE_TYPE.PARQUET)
        format2.setParquetGenerate(accregisterGenerate)

        var config: Array[FormatterConfig] = Array(
            format1,
            format2
        )
        var formatter = new TotalAccLoginFormatter(gameCode, logDate, config, false)
        formatter.setOutputPath(getOutputPath(logDate, Constants.PARQUET_2.TOTAL_ACC_LOGIN_OUTPUT_FOLDER))
        formatter.format(sc)
    }

    def totalAccPaidFormatter(logDate: String, sc: SparkContext): Unit = {
        var dateBeforeOneDay = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
        var pathList1 = getOutputPath(dateBeforeOneDay, Constants.PARQUET_2.TOTAL_ACC_PAID_OUTPUT_FOLDER, "data")
        var pathList2 = getOutputPath(logDate, Constants.PARQUET_2.FIRST_CHARGE_OUTPUT_FOLDER)

        def firstChargeGenerate(df:DataFrame): DataFrame = {
            df.select("game_code", "log_date", "id")
        }
        var sf = Constants.TOTAL_ACC_PAID
        var mapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID)

        var format1: FormatterConfig = new FormatterConfig(null, null, mapping, pathList1)
        format1.setInputFileType(Constants.INPUT_FILE_TYPE.PARQUET)
        var format2: FormatterConfig = new FormatterConfig(null, null, null, pathList2, Map("type" -> "union"))
        format2.setInputFileType(Constants.INPUT_FILE_TYPE.PARQUET)
        format2.setParquetGenerate(firstChargeGenerate)

        var config: Array[FormatterConfig] = Array(
            format1,
            format2
        )
        var formatter = new TotalAccPaidFormatter(gameCode, logDate, config, false)
        formatter.setOutputPath(getOutputPath(logDate, Constants.PARQUET_2.TOTAL_ACC_PAID_OUTPUT_FOLDER))
        formatter.format(sc)
    }

    def firstChargeFormatter(logDate: String, sc: SparkContext): Unit = {
        var dateBeforeOneDay = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)

        var pathList1 = getOutputPath(dateBeforeOneDay, Constants.PARQUET_2.TOTAL_ACC_PAID_OUTPUT_FOLDER, "data")
        var pathList2 = getOutputPath(logDate, Constants.PARQUET_2.PAYMENT_OUTPUT_FOLDER)

        var sF = Constants.FIRSTCHARGE_FIELD

        var on: Array[String] = Array(sF.ID)
        var where: Map[String, Map[String, Any]] = Map("A" -> Map(sF.ID -> "is null"))
        var fields: Map[String, Array[String]] = Map("B" -> Array(sF.GAME_CODE, sF.LOG_DATE, sF.PACKAGE_NAME, sF.CHANNEL, sF.PAY_CHANNEL, sF.ID, sF.DEVICE, sF.OS, sF.OS_VERSION))

        var format1: FormatterConfig = new FormatterConfig(null, null, null, pathList1)
        format1.setInputFileType(Constants.INPUT_FILE_TYPE.PARQUET)
        var format2: FormatterConfig = new FormatterConfig(null, null, null, pathList2, Map("type" -> "right join", "fields" -> fields, "where" -> where, "on" -> on))
        format2.setInputFileType(Constants.INPUT_FILE_TYPE.PARQUET)
        def customGenerate(dt:DataFrame): DataFrame = {
            dt.sort(dt("id"), dt("os").desc, dt("log_date").asc).dropDuplicates(Seq("id"))
        }
        format2.setCustomGenerate(customGenerate)

        var config: Array[FormatterConfig] = Array(
            format1,
            format2
        )
        var formatter = new FirstChargeFromPaying(gameCode, logDate, config, false)
        formatter.setOutputPath(getOutputPath(logDate, Constants.PARQUET_2.FIRST_CHARGE_OUTPUT_FOLDER))
        formatter.format(sc)
    }

    def roleRegisterFormatter(logDate: String, sc: SparkContext): Unit = {
        var dateBeforeOneDay = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
        var pathList1 = getOutputPath(dateBeforeOneDay, Constants.PARQUET_2.CHARACTER_INFO_OUTPUT_FOLDER, "data")
        var pathList2 = getOutputPath(logDate, Constants.PARQUET_2.LOGIN_LOGOUT_OUTPUT_FOLDER)

        var sF = Constants.ROLE_REGISTER_FIELD

        var on: Array[String] = Array(sF.ID, sF.RID)
        var where: Map[String, Map[String, Any]] = Map("A" -> Map(sF.ID -> "is null", sF.RID -> " is null"))
        var fields: Map[String, Array[String]] = Map("B" -> Array(sF.GAME_CODE, sF.LOG_DATE, sF.PACKAGE_NAME, sF.CHANNEL, sF.ID, sF.RID, sF.SID, sF.IP, sF.DEVICE, sF.OS, sF.OS_VERSION))

        var format1: FormatterConfig = new FormatterConfig(null, null, null, pathList1)
        format1.setInputFileType(Constants.INPUT_FILE_TYPE.PARQUET)
        var format2: FormatterConfig = new FormatterConfig(null, null, null, pathList2, Map("type" -> "right join", "fields" -> fields, "where" -> where, "on" -> on))
        format2.setInputFileType(Constants.INPUT_FILE_TYPE.PARQUET)
        format2.setFieldDistinct(Array("id", "sid", "rid"))

        var config: Array[FormatterConfig] = Array(
            format1,
            format2
        )
        var formatter = new NewRoleFromLogin(gameCode, logDate, config, false)
        formatter.setOutputPath(getOutputPath(logDate, Constants.PARQUET_2.ROLE_REGISTER_OUTPUT_FOLDER))
        formatter.format(sc)
    }
    
    def deviceRegisterFormatter(logDate: String, sc: SparkContext): Unit = {
        Common.logger("deviceRegisterFormatter start")
        var dateBeforeOneDay = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
        var path1 = getOutputPath(dateBeforeOneDay, Constants.PARQUET_2.TOTAL_DEVICE_LOGIN_OUTPUT_FOLDER, "data")
        var path2 = getOutputPath(logDate, Constants.PARQUET_2.LOGIN_LOGOUT_OUTPUT_FOLDER)
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
        var formatter = new NewDeviceFromLogin(gameCode, logDate, config, false)
        formatter.format(sc)
        formatter.setOutputPath(getOutputPath(logDate, Constants.PARQUET_2.DEVICE_REGISTER_OUTPUT_FOLDER))
        Common.logger("deviceRegisterFormatter end")
    }
    
    def deviceFirstChargeFormatter(logDate: String, sc: SparkContext): Unit = {
        Common.logger("deviceFirstChargeFormatter start")
        var dateBeforeOneDay = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
        var path1 = getOutputPath(dateBeforeOneDay, Constants.PARQUET_2.TOTAL_DEVICE_PAID_OUTPUT_FOLDER, "data")
        var path2 = getOutputPath(logDate, Constants.PARQUET_2.PAYMENT_OUTPUT_FOLDER)
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
        var formatter = new DeviceFirstChargeFromPaying(gameCode, logDate, config, false)
        formatter.format(sc)
        formatter.setOutputPath(getOutputPath(logDate, Constants.PARQUET_2.DEVICE_FIRST_CHARGE_OUTPUT_FOLDER))
        Common.logger("deviceFirstChargeFormatter end")
    }

    def getInputPath(logFolder: String, logDate: String, numberOfDay: Int, rootDir: String = INGAME_WAREHOUSE): String = {
        var rs = ""
        var hourly = ""
        if (mapParameters.contains("hourly")) {
            hourly = mapParameters("hourly")
        }
        if (hourly != "") {
            rs = getIngameHourlyPath(logFolder, logDate, numberOfDay, hourly)
        } else {
            rs = getIngameDailyPath(logFolder, logDate, numberOfDay)
        }
        rs = rootDir + "/" + gameFolder + "/" + rs
        rs
    }

    def getOutputPath(logDate: String, folderName: String, dataFolder: String = ""): String ={
        var outputFolder = "data"
        if(dataFolder != ""){
            outputFolder = dataFolder
        }else if(mapParameters.contains("hourly")){
            outputFolder = "data_hourly"
        }

        val outputPath = Constants.HOME_DIR + "/" + gameCode + "/ub/" + outputFolder + "/" + folderName + "/" + logDate
        outputPath
    }

    def getIngameHourlyPath(logFolder: String, logDate: String, numberOfFiles: Integer, hour: String): String = {
        val sb = new StringBuilder
        //sb.append(logFolder + "/{" + logDate + "_" + "*")
        sb.append(logFolder + "/{" + logDate + "_" + hour)
        for (i <- 1 to numberOfFiles) {
            val date = DateTimeUtils.getDateDifferent(i * -1, logDate, Constants.TIMING, Constants.A1);
            sb.append(",")
            sb.append(date)
            sb.append("_23")
        }
        sb.append("}")

        sb.toString()
    }

    def getIngameDailyPath(logFolder: String, logDate: String, numberOfFiles: Integer): String = {
        val sb = new StringBuilder
        sb.append(logFolder + "/{" + logDate)

        for (i <- 1 to numberOfFiles) {
            val date = DateTimeUtils.getDateDifferent(i * -1, logDate, Constants.TIMING, Constants.A1);
            sb.append(",")
            sb.append(date)
        }
        sb.append("}")
        sb.toString()
    }

    def createDoneFlag(logDate: String, sc: SparkContext): Unit = {
        if(!mapParameters.contains("logType")) {
            var path = ""
            if (mapParameters.contains("hourly")) {
                path = INGAME_WAREHOUSE + "/" + gameCode + "/ub/data_hourly/done-flag/" + logDate + "_" + mapParameters("hourly")
            } else {
                path = INGAME_WAREHOUSE + "/" + gameCode + "/ub/data/done-flag/" + logDate
            }
            Common.touchFile(path, sc)
        }
    }

    def getPartition(logDate: String, numOfDaily: Int = 2, numOfHourly: Int = 1): Array[String] = {
        var partition: Array[String] = Array()
        var dateFormat = DAILY_DS_FORMAT
        var numOf = numOfDaily
        if (mapParameters.contains("hourly")) {
            dateFormat = HOURLY_DS_FORMAT
            numOf = numOfHourly
        }
        partition = partition ++ Array(DateTimeUtils.formatDate(Constants.DEFAULT_DATE_FORMAT, dateFormat, logDate))
        for (i <- 1 to numOf - 1) {
            var dateBeforeOneDay = DateTimeUtils.getDateDifferent(-i, logDate, Constants.TIMING, Constants.A1)
            partition = partition ++ Array(DateTimeUtils.formatDate(Constants.DEFAULT_DATE_FORMAT, dateFormat, dateBeforeOneDay))
        }
        partition
    }
}
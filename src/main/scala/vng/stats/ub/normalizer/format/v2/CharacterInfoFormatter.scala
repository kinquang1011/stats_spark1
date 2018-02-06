package vng.stats.ub.normalizer.format.v2

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.utils.{DateTimeUtils, Common, Constants}
import org.apache.spark.sql.functions._
import scala.util.Try

/**
 * Created by tuonglv on 13/05/2016.
 */

class CharacterInfoFormatter(_gameCode:String, _logDate:String, _config: Array[FormatterConfig], _isSdkLog: Boolean = true)
    extends Formatter (_gameCode, _logDate, _config) {
    var schema: List[List[Any]] = List(
        List(Constants.CHARACTER_INFO_FIELD.GAME_CODE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.CHARACTER_INFO_FIELD.LOG_DATE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.CHARACTER_INFO_FIELD.SID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.CHARACTER_INFO_FIELD.ID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.CHARACTER_INFO_FIELD.RID, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.CHARACTER_INFO_FIELD.LEVEL, Constants.DATA_TYPE_INTEGER, Constants.ENUM0),
        List(Constants.CHARACTER_INFO_FIELD.ONLINE_TIME, Constants.DATA_TYPE_LONG, Constants.ENUM0),
        List(Constants.CHARACTER_INFO_FIELD.LAST_LOGIN_DATE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.CHARACTER_INFO_FIELD.REGISTER_DATE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.CHARACTER_INFO_FIELD.FIRST_CHARGE_DATE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.CHARACTER_INFO_FIELD.LAST_CHARGE_DATE, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.CHARACTER_INFO_FIELD.TOTAL_CHARGE, Constants.DATA_TYPE_DOUBLE, Constants.ENUM0),
        List(Constants.CHARACTER_INFO_FIELD.MONEY_BALANCE, Constants.DATA_TYPE_DOUBLE, Constants.ENUM0),
        List(Constants.CHARACTER_INFO_FIELD.FIRST_LOGIN_CHANNEL, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.CHARACTER_INFO_FIELD.FIRST_CHARGE_CHANNEL, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING),
        List(Constants.CHARACTER_INFO_FIELD.MORE_INFO, Constants.DATA_TYPE_STRING, Constants.DATA_EMPTY_STRING)

    )
    monitorCode = "char_info"
    setParquetSchema(schema)
    outputPath = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.CHARACTER_INFO_OUTPUT_FOLDER, logDate, _isSdkLog)

    def setOutputPath(_outputPath: String): Unit = {
        outputPath = _outputPath
    }

    def loadSnapshot(sqlContext: SQLContext): DataFrame = {
        var dateBeforeOneDay = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
        var characterInfoBefore = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.CHARACTER_INFO_OUTPUT_FOLDER, dateBeforeOneDay, false)
        var characterInfoDF: DataFrame = null
        Try {
            characterInfoDF = sqlContext.read.parquet(characterInfoBefore)
        }
        characterInfoDF
    }

    def loadNewRegister(sqlContext: SQLContext): DataFrame = {
        var unionPath = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.ROLE_REGISTER_OUTPUT_FOLDER, logDate, false)
        var unionDataFrame: DataFrame = null
        var bc = Try {
            unionDataFrame = sqlContext.read.parquet(unionPath)
        }
        if (bc.isFailure) {
            unionPath = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.ACC_REGISTER_OUTPUT_FOLDER, logDate, false)
            unionDataFrame = sqlContext.read.parquet(unionPath)
        }
        unionDataFrame  = unionDataFrame.selectExpr("*", "cast (log_date as string) as register_date", "cast (channel as string) as first_login_channel")
        unionDataFrame
    }


    def getInfoFromPayment(_characterInfo: DataFrame, sqlContext: SQLContext): DataFrame = {
        var characterInfo = _characterInfo

        var paymentPath = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.PAYMENT_OUTPUT_FOLDER, logDate, false)
        var paymentDF = sqlContext.read.parquet(paymentPath)

        //total charge in logDate
        var pdf1 = paymentDF.groupBy("id", "rid", "sid").agg(sum("net_amt").as('total_net_amt))
        //first charge
        var pdf2 = paymentDF.sort(asc("id"), asc("rid"), asc("sid"), asc("log_date")).dropDuplicates(Seq("id", "rid", "sid"))
        //last charge
        var pdf3 = paymentDF.sort(asc("id"), asc("rid"), asc("sid"), desc("log_date")).dropDuplicates(Seq("id", "rid", "sid"))

        var join12 = pdf1.as('pdf1).join(pdf2.as('pdf2),
            pdf1("id") === pdf2("id") and pdf1("rid") === pdf2("rid") and pdf1("sid") === pdf2("sid"),
            "left_outer").selectExpr("pdf1.id", "pdf1.sid", "pdf1.rid", "pdf1.total_net_amt as n_total_charge", "pdf2.log_date as n_first_charge_date", "pdf2.channel as n_first_charge_channel")

        var join123 = join12.as('join12).join(pdf3.as('pdf3),
            join12("id") === pdf3("id") and join12("rid") === pdf3("rid") and join12("sid") === pdf3("sid"),
            "left_outer").selectExpr("join12.*", "pdf3.log_date as n_last_charge_date")

        characterInfo = characterInfo.as('c).join(join123.as('join123),
            characterInfo("id") === join123("id") and characterInfo("rid") === join123("rid") and characterInfo("sid") === join123("sid"),
            "left_outer").selectExpr("c.*", "join123.n_total_charge", "join123.n_first_charge_date", "join123.n_last_charge_date","join123.n_first_charge_channel")

        characterInfo = characterInfo.withColumn("n_total_charge",
            when(!characterInfo("n_total_charge").isNull,characterInfo("n_total_charge")).otherwise(0L))

        characterInfo = characterInfo.withColumn("total_charge",
            characterInfo("n_total_charge") + characterInfo("total_charge"))

        characterInfo = characterInfo.withColumn("first_charge_date",
            when(characterInfo("first_charge_date").isNull || characterInfo("first_charge_date") === "",characterInfo("n_first_charge_date")).otherwise(characterInfo("first_charge_date")))

        characterInfo = characterInfo.withColumn("first_charge_channel",
            when(characterInfo("first_charge_channel").isNull || characterInfo("first_charge_channel") === "",characterInfo("n_first_charge_channel")))

        characterInfo = characterInfo.withColumn("last_charge_date",
            when(characterInfo("n_last_charge_date") > characterInfo("last_charge_date"), characterInfo("n_last_charge_date")).otherwise(characterInfo("last_charge_date")))

        characterInfo = characterInfo.drop("n_total_charge").drop("n_online_time").drop("n_first_charge_date").drop("n_first_charge_channel")

        characterInfo
    }

    def getInfoFromActivity(_characterInfo: DataFrame, sqlContext: SQLContext): DataFrame = {
        var characterInfo = _characterInfo

        var activityPath = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.LOGIN_LOGOUT_OUTPUT_FOLDER, logDate, false)
        var activityDF = sqlContext.read.parquet(activityPath)

        //last login
        var adf1 = activityDF.filter("action='login'").sort(asc("id"), asc("rid"), asc("sid"), desc("log_date")).dropDuplicates(Seq("id", "rid", "sid"))
        //last level
        var adf2 = activityDF.filter("action='logout'").sort(asc("id"), asc("rid"), asc("sid"), desc("log_date")).dropDuplicates(Seq("id", "rid", "sid"))
        //total online time
        var adf3 = activityDF.filter("action='logout'").groupBy("id", "rid", "sid").agg(sum("online_time").as('total_online_time))

        var join23 = adf2.as('adf2).join(adf3.as('adf3),
            adf2("id") === adf3("id") and adf2("rid") === adf3("rid") and adf2("sid") === adf3("sid"),
            "left_outer").selectExpr("adf2.id", "adf2.sid", "adf2.rid", "adf2.level", "adf3.total_online_time")

        characterInfo = characterInfo.as('c).join(join23.as('join23),
            characterInfo("id") === join23("id") and characterInfo("rid") === join23("rid") and characterInfo("sid") === join23("sid"),
            "left_outer").selectExpr("c.*", "join23.level as n_level", "join23.total_online_time as n_online_time")

        characterInfo = characterInfo.as('c).join(adf1.as('adf1),
            characterInfo("id") === adf1("id") and characterInfo("rid") === adf1("rid") and characterInfo("sid") === adf1("sid"),
            "left_outer").selectExpr("c.*", "adf1.log_date as n_last_login_date")

        characterInfo = characterInfo.withColumn("level",
            when(characterInfo("n_level") > characterInfo("level"), characterInfo("n_level")).otherwise(characterInfo("level")))

        characterInfo = characterInfo.withColumn("n_online_time",
            when(!characterInfo("n_online_time").isNull,characterInfo("n_online_time")).otherwise(0L))

        characterInfo = characterInfo.withColumn("online_time",
            characterInfo("n_online_time") + characterInfo("online_time"))

        characterInfo = characterInfo.withColumn("last_login_date",
            when(characterInfo("n_last_login_date") > characterInfo("last_login_date"), characterInfo("n_last_login_date")).otherwise(characterInfo("last_login_date")))

        characterInfo = characterInfo.drop("n_level").drop("n_online_time").drop("n_last_login_date")
        characterInfo
    }

    override def format(sc: SparkContext) {
        val sqlContext = new SQLContext(sc)
        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
        sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")

        var characterInfoDF: DataFrame = loadSnapshot(sqlContext)
        var unionDataFrame: DataFrame = loadNewRegister(sqlContext)

        var characterInfoTotalDF: DataFrame = null
        var unionDF: DataFrame = null
        if (unionDataFrame != null) {
            var unionField = Array("id", "rid", "sid", "first_login_channel", "register_date")
            val parquetField = getParquetField(unionField)
            unionDF = convertToParquetType(unionDataFrame, parquetField)
        }

        if (unionDF != null && characterInfoDF != null) {
            characterInfoTotalDF = characterInfoDF.unionAll(unionDF)
        } else if (characterInfoDF != null) {
            characterInfoTotalDF = characterInfoDF
        } else if (unionDF != null) {
            characterInfoTotalDF = unionDF
        }

        if (characterInfoTotalDF != null) {
            characterInfoTotalDF = getInfoFromActivity(characterInfoTotalDF, sqlContext)
            characterInfoTotalDF = getInfoFromPayment(characterInfoTotalDF, sqlContext)
            characterInfoTotalDF = characterInfoTotalDF.drop("game_code").drop("log_date").selectExpr("cast ('" + gameCode + "' as string) as game_code",
                "cast ('" + logDate + "' as string) as log_date", "*")
            var fullCharacterInfoMapping = getFullSchema()
            writeParquet(characterInfoTotalDF, fullCharacterInfoMapping, sqlContext, sc)
        }
    }
}

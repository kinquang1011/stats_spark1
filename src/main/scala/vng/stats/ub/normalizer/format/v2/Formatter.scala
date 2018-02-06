package vng.stats.ub.normalizer.format.v2

import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import vng.stats.ub.db.MysqlDB
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.utils.{Common, Constants}

import scala.util.Try

/**
 * Created by tuonglv on 13/05/2016.
 */
class Formatter (_gameCode:String, _logDate:String, _config: Array[FormatterConfig]) extends Serializable {
    var writeMode = "overwrite"
    var writeFormat = "parquet"
    var monitorCode = "formater"
    val config: Array[FormatterConfig] = _config
    var logDate = _logDate
    val gameCode = _gameCode
    var multipleDate = false

    var parquetSchema: List[List[Any]] = null
    var outputPath = ""

    protected var _sqlContext: SQLContext = null

    def setWriteMode(_writeMode: String): Unit = {
        writeMode = _writeMode
    }

    def setParquetSchema(_schema: List[List[Any]]): Unit = {
        parquetSchema = _schema
    }

    def setWriteFormat(_writeFormat: String): Unit = {
        writeFormat = _writeFormat
    }

    def format(sc: SparkContext): Unit = {
        //val sqlContext = new SQLContext(sc)
        //sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
        //sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")

        val hiveContext = new HiveContext(sc)
        hiveContext.setConf("spark.sql.parquet.compression.codec", "snappy")
        hiveContext.setConf("spark.sql.parquet.binaryAsString", "true")

        val sqlContext = hiveContext
        _sqlContext = sqlContext

        var finalData: DataFrame = null

        var fullMapping: Array[String] = Array()

        config.foreach { formatConfig =>
            val filePath = formatConfig.filePath
            val filter = formatConfig.filter
            val generate = formatConfig.generate
            val rel = formatConfig.rel
            val delimiter = formatConfig.delimiter
            val charDelimiter = formatConfig.charDelimiter
            val mappingConfig = formatConfig.mappingConfig
            val inputFileType = formatConfig.inputFileType
            var fieldDistinct = formatConfig.fieldDistinct
            var partition = formatConfig.coalescePartition
            var hiveQueryConfig = formatConfig.hiveQueryConfig
            var customGenerate = formatConfig.customGenerate

            Common.logger("Processing for path: " + filePath)

            var objDF: DataFrame = null
            if (inputFileType == Constants.INPUT_FILE_TYPE.TSV) {
                var rawData: RDD[Array[String]] = null
                fullMapping = fullMapping ++ mappingConfig
                if (charDelimiter == '\0') {
                    rawData = sc.textFile(filePath).map(_.split(delimiter)).filter { row =>
                        filter(row)
                    }
                } else {
                    rawData = sc.textFile(filePath).map(_.split(charDelimiter)).filter { row =>
                        filter(row)
                    }
                }
                val objRDD = rawData.map { row =>
                    generate(row)
                }
                val schema = getSchema(mappingConfig)
                objDF = sqlContext.createDataFrame(objRDD, schema)
            } else if (inputFileType == Constants.INPUT_FILE_TYPE.PARQUET) {
                if (mappingConfig != null) {
                    fullMapping = fullMapping ++ mappingConfig
                }
                Try {
                    objDF = sqlContext.read.parquet(filePath)
                    if (formatConfig.parquetGenerate != null) {
                        objDF = formatConfig.parquetGenerate(objDF)
                    }
                }
            } else if (inputFileType == Constants.INPUT_FILE_TYPE.HIVE) {
                Common.logger("Hive query: " + hiveQueryConfig.build_query)
                if (hiveQueryConfig._udfRegister.size != 0) {
                    hiveQueryConfig._udfRegister.keys.foreach { funcName =>
                        hiveContext.udf.register(funcName, hiveQueryConfig._udfRegister(funcName))
                    }
                }
                objDF = hiveContext.sql(hiveQueryConfig.build_query)
                fullMapping = fullMapping ++ hiveQueryConfig.getMapping()
            } else if (inputFileType == Constants.INPUT_FILE_TYPE.JSON) {
                objDF = sqlContext.read.json(filePath)
                var jsonConfig = formatConfig.jsonFormatConfig
                objDF = jsonConfig.filter(objDF)
                objDF = jsonConfig.generate(objDF)
                if (mappingConfig != null) {
                    fullMapping = fullMapping ++ mappingConfig
                }
            }
            if (objDF != null && objDF.count != 0) {
                objDF = dropDuplicate(objDF, fieldDistinct)
                objDF = coalescePartition(objDF, partition)
                if (customGenerate != null) {
                    objDF = customGenerate(objDF)
                }
            }
            if (finalData == null || rel == null || !rel.contains("type") || rel("type") == "union") {
                if (objDF != null) {
                    if (finalData == null) {
                        finalData = objDF
                    } else {
                        finalData = finalData.unionAll(objDF)
                    }
                } else {
                    /*payment = null
                    neu payment = null va type = join (type != union)
                    if(rel != null && rel.contains("type") && rel("type") != "union") {
                        Common.logger("Dataset 2 is null, set finalData is null")
                        finalData = null
                    }*/
                }
            } else if (finalData != null && objDF != null) {
                val on = rel("on").asInstanceOf[Array[String]]
                val onSql = createOnSql(on)
                val fields = rel("fields").asInstanceOf[Map[String, Array[String]]]
                val fieldsSql = createFieldSql(fields)
                val where = rel("where").asInstanceOf[Map[String, Map[String, Any]]]
                val whereSql = createWhereSql(where)
                val relType = rel("type") // join , left join, right join
                finalData.registerTempTable("AA")
                objDF.registerTempTable("BB")
                var sqlExe = "select " + fieldsSql + " from AA as A " + relType + " BB as B on " + onSql
                if (whereSql != "") {
                    sqlExe = sqlExe + " where " + whereSql
                }
                Common.logger("execute sql: " + sqlExe)
                val data = sqlContext.sql(sqlExe)
                finalData = data
                sqlContext.dropTempTable("AA")
                sqlContext.dropTempTable("BB")
            } else {
                Common.logger("Dataframe is null in formatter, set finalData = null")
                finalData = null
            }
        }
        writeParquet(finalData, fullMapping, sqlContext, sc)
    }

    def coalescePartition(objDF: DataFrame, partition: Int): DataFrame = {
        var rs = objDF
        if (partition != 0) {
            Common.logger("coalesce = " + partition)
            rs = rs.coalesce(partition)
        }
        rs
    }

    def dropDuplicate(objDF: DataFrame, fieldDistinct: Array[String]): DataFrame = {
        var rs: DataFrame = null
        if (objDF != null) {
            var size = fieldDistinct.size
            Common.logger("drop duplicate with size = " + size)
            if (size == 1) {
                rs = objDF.sort("log_date", fieldDistinct(0)).dropDuplicates(Seq(fieldDistinct(0)))
            } else if (size == 2) {
                rs = objDF.sort("log_date", fieldDistinct(0), fieldDistinct(1)).dropDuplicates(Seq(fieldDistinct(0), fieldDistinct(1)))
            } else if (size == 3) {
                rs = objDF.sort("log_date", fieldDistinct(0), fieldDistinct(1), fieldDistinct(2)).dropDuplicates(Seq(fieldDistinct(0), fieldDistinct(1), fieldDistinct(2)))
            } else {
                rs = objDF
            }
        }
        rs
    }

    def verifyData(data: DataFrame): Boolean = {
        true
    }

    def writeParquet(finalData: DataFrame, fullMapping: Array[String], sqlContext: SQLContext, sc: SparkContext): Unit = {
        var write: DataFrame = null
        if (finalData != null) {
            val parquetField = getParquetField(fullMapping.distinct)
            write = convertToParquetType(finalData, parquetField)
            write.cache
            var b: Boolean = verifyData(write)
            if (!b) {
                Common.logger("verifyData return false, logDate = " + logDate)
                return
            }
            if (!multipleDate) {
                Common.logger("Final data not null, data will be write in " + outputPath)
                write.coalesce(1).write.mode(writeMode).format(writeFormat).save(outputPath)
            } else {
                writeMultipleDate(write, sqlContext)
            }
            write.unpersist()
        } else {
            writeSchemaWithoutData(sc, sqlContext)
            Common.logger("Data is null, write schema without data, path = " + outputPath)
        }
    }

    def writeMultipleDate(data: DataFrame, sqlContext: SQLContext): Unit = {
        data.cache()
        data.selectExpr("date_format(log_date,\"yyyy-MM-dd\") as f_log_date").distinct.collect().foreach { _log_date =>
            var t1 = _log_date.toString().dropRight(1)
            var t2 = t1.drop(1)
            var m_outputPath = outputPath.replace(logDate, t2)
            Common.logger("writeMultipleDate, date = " + t2)
            Common.logger("old path = " + outputPath)
            Common.logger("new path = " + m_outputPath)
            var smallData = data.filter("log_date >= '" + t2 + " 00:00:00' and log_date <= '" + t2 + " 23:59:59'")
            if (verifyData(smallData)) {
                smallData.coalesce(1).write.mode(writeMode).format(writeFormat).save(m_outputPath)
            } else {
                Common.logger("verifyData return false, logDate = " + t2)
            }
        }
        data.unpersist()
    }

    def getSchema(mappingConfig: Array[String]): StructType = {
        val schema =
            StructType(
                mappingConfig.mkString(",").split(",").map(fieldName => StructField(fieldName, StringType, true)))
        schema
    }

    def getDefaultSchema(): StructType = {
        var aF: Array[StructField] = Array()
        parquetSchema.foreach { list =>
            val field_name: String = list(0).toString
            val field_type = list(1)
            field_type match {
                case Constants.DATA_TYPE_DOUBLE =>
                    aF = aF ++ Array(StructField(field_name, DoubleType, true))
                case Constants.DATA_TYPE_LONG =>
                    aF = aF ++ Array(StructField(field_name, LongType, true))
                case Constants.DATA_TYPE_INTEGER =>
                    aF = aF ++ Array(StructField(field_name, IntegerType, true))
                case Constants.DATA_TYPE_STRING =>
                    aF = aF ++ Array(StructField(field_name, StringType, true))
            }
        }
        val _schema = StructType(aF)
        _schema
    }

    def writeSchemaWithoutData(sc: SparkContext, sqlContext: SQLContext): Unit = {
        var b: RDD[Row] = sc.makeRDD(Seq(Row("2016", "10", "us"), Row("a"), Row("a")))
        var schemaWithoutData: DataFrame = sqlContext.createDataFrame(b, getDefaultSchema)
        var schemaWithoutData1 = schemaWithoutData.filter("game_code='1'")
        val parquetField = getParquetField(Array("a"))
        var write = convertToParquetType(schemaWithoutData1, parquetField)
        write.coalesce(1).write.mode(writeMode).format(writeFormat).save(outputPath)
    }

    /*
    def getSampleRDDRow(df: DataFrame,sc:SparkContext) : RDD[Row] = {
        var length = df.columns.length
        var seq: Seq[String] = Seq()
        for (i <- 1 to length) {
            seq = seq :+ "aa"
        }
        var rdd:RDD[Row] = sc.parallelize(Seq("1","2"))
        val rowRdd:RDD[Row] = rdd.map(v => Row(v: _*))
        rowRdd
    }
    */
    //tam thoi hardcode cho toi khi tim dc cach tot hon
    def convertToParquetType(finalData: DataFrame, p: Array[String]): DataFrame = {
        val length = p.length
        val returnDF = length match {
            case 1 =>
                finalData.selectExpr(p(0))
            case 2 =>
                finalData.selectExpr(p(0), p(1))
            case 3 =>
                finalData.selectExpr(p(0), p(1), p(2))
            case 4 =>
                finalData.selectExpr(p(0), p(1), p(2), p(3))
            case 5 =>
                finalData.selectExpr(p(0), p(1), p(2), p(3), p(4))
            case 6 =>
                finalData.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5))
            case 7 =>
                finalData.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6))
            case 8 =>
                finalData.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7))
            case 9 =>
                finalData.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8))
            case 10 =>
                finalData.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9))
            case 11 =>
                finalData.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10))
            case 12 =>
                finalData.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11))
            case 13 =>
                finalData.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12))
            case 14 =>
                finalData.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13))
            case 15 =>
                finalData.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14))
            case 16 =>
                finalData.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15))
            case 17 =>
                finalData.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16))
            case 18 =>
                finalData.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17))
            case 19 =>
                finalData.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18))
            case 20 =>
                finalData.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), p(19))

        }
        returnDF
    }

    //fullSchema: Array("log_date", "id", "did", "channel")
    //"updatetime", "userID", "device_id", "type"
    def getParquetField(fullSchema: Array[String]): Array[String] = {
        var arrReturn: Array[String] = Array()
        parquetSchema.foreach { list =>
            val field = list(0)
            val dataType = list(1)
            val defaultValue = list(2)

            if (fullSchema.contains(field)) {
                var t = "cast (" + field + " as " + dataType + ") as " + field
                arrReturn = arrReturn ++ Array(t)
            } else {
                var t = "cast (" + defaultValue + " as " + dataType + ") as " + field
                arrReturn = arrReturn ++ Array(t)
            }
        }
        arrReturn
    }

    def createOnSql(on: Array[String]): String = {
        var sql: String = ""
        for (f <- on) {
            sql = sql + "A." + f + " = B." + f + " and "
        }
        sql.dropRight(5)
    }

    def createWhereSql(where: Map[String, Map[String, Any]]): String = {
        var sql: String = ""
        where.keys.foreach { table =>
            val f = where(table)
            f.keys.foreach { k =>
                val v = f(k)
                sql = sql + table + "." + k + " " + v + " and "
            }
        }
        sql.dropRight(5)
    }

    def createFieldSql(fields: Map[String, Array[String]]): String = {
        var sql = ""
        fields.keys.foreach { table =>
            val f_arr = fields(table)
            f_arr.foreach { f =>
                sql = sql + table + "." + f + ", "
            }
        }
        sql.dropRight(2)
    }

    def insertMonitorLog(monitorCode: String, message: String, level: String = "warning"): Unit = {
        var tableName = "fw_monitor"
        val now = Calendar.getInstance().getTime()
        val log_date_format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val messageDate = log_date_format.format(now)

        val sql = "insert into " + tableName + " (game_code, monitor_code, level, monitor_message, log_date, message_date) values(" +
            "'" + gameCode + "','" + monitorCode + "','" + level + "','" + message + "','" + logDate + "','" + messageDate + "')"

        val mysqlDB = new MysqlDB()
        mysqlDB.executeUpdate(sql, true) //execute sql and then close
        mysqlDB.close()
    }

    def getFullSchema(): Array[String] = {
        var rs: Array[String] = Array()
        parquetSchema.foreach { list =>
            rs = rs ++ Array(list(0).toString)
        }
        rs
    }
}

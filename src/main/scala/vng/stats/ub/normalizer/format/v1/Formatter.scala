package vng.stats.ub.normalizer.format.v1

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import vng.stats.ub.utils.{Common, Constants}

import scala.util.Try

/**
 * Created by tuonglv on 13/05/2016.
 */
class Formatter (_gameCode:String, _logDate:String, _config: Array[FormatterConfig]) extends Serializable{
    var writeMode = "overwrite"
    var writeFormat = "parquet"
    var outputFolder = ""

    val config: Array[FormatterConfig] = _config
    var logDate = _logDate
    val gameCode = _gameCode

    var isSdkLog = false

    var parquetSchema: List[List[Any]] = null

    def setWriteMode(_writeMode:String): Unit = {
        writeMode = _writeMode
    }

    def setSdkLog(flag: Boolean): Unit ={
        isSdkLog = flag
    }

    def setParquetSchema(_schema:List[List[Any]]): Unit ={
        parquetSchema = _schema
    }

    def setOutputFolder(_folderName: String): Unit ={
        outputFolder = _folderName
    }

    def setWriteFormat(_writeFormat:String): Unit ={
        writeFormat = _writeFormat
    }

    def format(sc: SparkContext): Unit = {
        val sqlContext = new SQLContext(sc)
        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
        sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")

        var finalData: DataFrame = null

        var fullMapping:Array[String] = null

        config.foreach { formatConfig =>
            val filePath = formatConfig.filePath
            val filter = formatConfig.filter
            val generate = formatConfig.generate
            val rel = formatConfig.rel
            val delimiter = formatConfig.delimiter
            val charDelimiter = formatConfig.charDelimiter
            val mappingConfig = formatConfig.mappingConfig
            val inputFileType = formatConfig.inputFileType
            
            if (fullMapping == null) {
                fullMapping = mappingConfig
            } else {
                // vinhdp
                if(mappingConfig != null){
                
                    fullMapping = fullMapping ++ mappingConfig
                }
            }

            var objDF:DataFrame = null
            if(inputFileType == "tsv"){
                
                //if(DataUtils.isExist(filePath)){
                    
                    var rawData: RDD[Array[String]] = null
                    
                    if(charDelimiter == '\0'){
                        rawData = sc.textFile(filePath).map(_.split(delimiter)).filter{row => 
                        
                            filter(row)
                        }
                    }else{
                        rawData = sc.textFile(filePath).map(_.split(charDelimiter)).filter{row => 
                        
                            filter(row)
                        }
                    }
                    val objRDD = rawData.map { row =>
                        generate(row)
                    }
                    val schema = getSchema(mappingConfig)
                    objDF = sqlContext.createDataFrame(objRDD, schema)
                /*}else{
                    Common.logger("VINHDP -> " + filePath + " NOTFOUND")
                }*/
                
            }else { // parquet
                Try{
                    objDF = sqlContext.read.parquet(filePath)
                }
            }

            if (finalData == null || rel == null || !rel.contains("type") || rel("type") == "union") {
                if(objDF!=null){
                    if(finalData==null){
                        finalData = objDF
                    }else{
                        finalData = finalData.unionAll(objDF)
                    }
                }
            } else if(finalData != null && objDF != null){
                val on = rel("on").asInstanceOf[Array[String]]
                val onSql = createOnSql(on)
                val fields = rel("fields").asInstanceOf[Map[String, Array[String]]]
                val fieldsSql = createFieldSql(fields)
                val where = rel("where").asInstanceOf[Map[String, Map[String, Any]]]
                val whereSql = createWhereSql(where)
                val relType = rel("type") // join , left join, right join
                finalData.registerTempTable("AA")
                objDF.registerTempTable("BB")
                val sqlExe = "select " + fieldsSql + " from AA as A " + relType + " BB as B on " + onSql + " where " + whereSql
                val data = sqlContext.sql(sqlExe)
                finalData = data
                sqlContext.dropTempTable("AA")
                sqlContext.dropTempTable("BB")
            }else{
                Common.logger("Dataframe is null in formatter")
            }
        }
        writeParquet(finalData,fullMapping,sqlContext,sc)
    }

    def writeParquet(finalData:DataFrame, fullMapping:Array[String], sqlContext: SQLContext, sc:SparkContext): Unit ={
        var write: DataFrame = null
        var outputPath=Common.getOuputParquetPath(gameCode,outputFolder,logDate, isSdkLog)
        if (finalData != null) {
            val parquetField = getParquetField(fullMapping.distinct)
            write = convertToParquetType(finalData,parquetField)
            write.coalesce(1).write.mode(writeMode).format(writeFormat).save(outputPath)
        } else {
            val schemaWithoutData: DataFrame = sqlContext.createDataFrame(sc.emptyRDD[Row],getDefaultSchema)
            schemaWithoutData.coalesce(1).write.mode(writeMode).format(writeFormat).save(outputPath)
            println("Data is null, write schema without data, path = " + outputFolder)
        }
    }

    def getSchema(mappingConfig: Array[String]): StructType ={
        val schema =
            StructType(
                mappingConfig.mkString(",").split(",").map(fieldName => StructField(fieldName, StringType, true)))
        schema
    }

    def getDefaultSchema(): StructType ={
        var aF: Array[StructField] = Array()
        parquetSchema.foreach{ list =>
            val field_name: String = list(0).toString
            val field_type = list(1)
            field_type match {
                case Constants.DATA_TYPE_DOUBLE =>
                    aF = aF ++ Array(StructField(field_name, DoubleType,true))
                case Constants.DATA_TYPE_LONG =>
                    aF = aF ++ Array(StructField(field_name, LongType,true))
                case Constants.DATA_TYPE_INTEGER =>
                    aF = aF ++ Array(StructField(field_name, IntegerType,true))
                case Constants.DATA_TYPE_STRING =>
                    aF = aF ++ Array(StructField(field_name, StringType,true))
            }
        }
        val _schema = StructType(aF)
        _schema
    }

    //tam thoi hardcode cho toi khi tim dc cach tot hon
    def convertToParquetType(finalData: DataFrame, p: Array[String]) : DataFrame = {
        val length = p.length
        val returnDF = length match {
            case 1 =>
                finalData.selectExpr(p(0))
            case 2 =>
                finalData.selectExpr(p(0),p(1))
            case 3 =>
                finalData.selectExpr(p(0),p(1),p(2))
            case 4 =>
                finalData.selectExpr(p(0),p(1),p(2),p(3))
            case 5 =>
                finalData.selectExpr(p(0),p(1),p(2),p(3),p(4))
            case 6 =>
                finalData.selectExpr(p(0),p(1),p(2),p(3),p(4),p(5))
            case 7 =>
                finalData.selectExpr(p(0),p(1),p(2),p(3),p(4),p(5),p(6))
            case 8 =>
                finalData.selectExpr(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7))
            case 9 =>
                finalData.selectExpr(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8))
            case 10 =>
                finalData.selectExpr(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9))
            case 11 =>
                finalData.selectExpr(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9),p(10))
            case 12 =>
                finalData.selectExpr(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9),p(10),p(11))
            case 13 =>
                finalData.selectExpr(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9),p(10),p(11),p(12))
            case 14 =>
                finalData.selectExpr(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9),p(10),p(11),p(12),p(13))
            case 15 =>
                finalData.selectExpr(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9),p(10),p(11),p(12),p(13),p(14))
            case 16 =>
                finalData.selectExpr(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9),p(10),p(11),p(12),p(13),p(14),p(15))
            case 17 =>
                finalData.selectExpr(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9),p(10),p(11),p(12),p(13),p(14),p(15),p(16))
            case 18 =>
                finalData.selectExpr(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9),p(10),p(11),p(12),p(13),p(14),p(15),p(16),p(17))

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

            if(fullSchema.contains(field)){
                arrReturn = arrReturn ++ Array("cast (" + field + " as " + dataType + ") as " + field)
            }else{
                //arrReturn = arrReturn ++ Array(defaultValue + " as "  + field)
                arrReturn = arrReturn ++ Array("cast (" + defaultValue + " as " + dataType + ") as " + field)
            }
        }
        arrReturn
    }

    def createOnSql(on: Array[String]): String = {
        var sql: String = ""
        for(f <- on){
            sql = sql + "A." + f + " = B." + f + " and "
        }
        sql.dropRight(5)
    }
    def createWhereSql(where: Map[String,Map[String,Any]]): String = {
        var sql: String = ""
        where.keys.foreach{ table =>
            val f = where(table)
            f.keys.foreach { k =>
                val v = f(k)
                sql = sql + table + "." + k + " " + v + " and "
            }
        }
        sql.dropRight(5)
    }
    def createFieldSql(fields: Map[String,Array[String]]): String = {
        var sql=""
        fields.keys.foreach{ table =>
            val f_arr = fields(table)
            f_arr.foreach{ f =>
                sql =  sql + table + "." + f + ", "
            }
        }
        sql.dropRight(2)
    }
}

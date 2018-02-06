package vng.stats.ub.normalizer.format.v1

/**
 * Created by tuonglv on 18/05/2016.
 */
import org.apache.spark.sql.{DataFrame, Row}
import vng.stats.ub.normalizer.format.v2.{JsonFormatConfig, HiveQueryConfig}

class FormatterConfig(_filter: (Array[String]) => Boolean,_generate: (Array[String]) => Row,
                       _mapping:Array[String], _filePath: String, _rel:Map[String,Any] = null, _delimiter: String = "\t") extends Serializable {

    var configName = ""
    var generate = _generate
    var filePath = _filePath
    var rel = _rel
    var delimiter = _delimiter
    var charDelimiter: Char = '\0'
    var mappingConfig = _mapping
    var filter = _filter
    var inputFileType = "tsv"
    var extraTime = 0L
    var convertRate = 1.0
    
    var fieldDistinct:Array[String] = Array()
    var coalescePartition = 0
    var hiveQueryConfig: HiveQueryConfig = new HiveQueryConfig()
    var jsonFormatConfig: JsonFormatConfig = new JsonFormatConfig()
    var parquetGenerate: (DataFrame) => DataFrame = null
    var customGenerate:(DataFrame) => DataFrame = null

    def setParquetGenerate(_parquetGenerate: (DataFrame) => DataFrame): Unit ={
        parquetGenerate = _parquetGenerate
    }
    def setCustomGenerate(_customGenerate: (DataFrame) => DataFrame): Unit ={
        customGenerate = _customGenerate
    }

    def setCoalescePartition(_partition:Int): Unit ={
        coalescePartition = _partition
    }
    def setConfigName(_configName: String): Unit ={
        configName = _configName
    }
    def setExtraTime(extra: Long): Unit ={
        extraTime = extra
    }
    def setConvertRate(rate: Double): Unit ={
        convertRate = rate
    }
    def setInputFileType(_fileType: String): Unit ={
        inputFileType = _fileType
    }
    def setDelimiter(delimiter: String): Unit = {
        this.delimiter = delimiter
    }
    def setCharDelimiter(delimiter: Char): Unit = {
        this.charDelimiter = delimiter
    }
    def setFieldDistinct(_fieldDistinct: Array[String]): Unit ={
        fieldDistinct = _fieldDistinct
    }

}
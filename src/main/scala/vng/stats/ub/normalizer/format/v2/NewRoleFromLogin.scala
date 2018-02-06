package vng.stats.ub.normalizer.format.v2

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.utils.{Common, Constants, DateTimeUtils}

import scala.util.Try

/**
 * Created by tuonglv on 30/05/2016.
 */
class NewRoleFromLogin(_gameCode:String, _logDate:String, _config: Array[FormatterConfig], _isSdkLog: Boolean = true)
    extends RoleRegisterFormatter (_gameCode, _logDate, _config, _isSdkLog) {
    monitorCode = "nrfl"
    var characterInfoBeforePath = ""
    var fieldsSelected: Array[String] = Array()

    setFieldsSelected(_config)

    def setFieldsSelected(config: Array[FormatterConfig]): Unit = {
        config.foreach { formatConfig =>
            if (formatConfig.rel != null) {
                val rel = formatConfig.rel
                val fields = rel("fields").asInstanceOf[Map[String, Array[String]]]
                fields.keys.foreach { table =>
                    val f_arr = fields(table)
                    f_arr.foreach { f =>
                        fieldsSelected = fieldsSelected ++ Array(f)
                    }
                }
            }
        }
    }

    override def writeParquet(finalData: DataFrame, fullSchemaArr: Array[String], sqlContext: SQLContext, sc: SparkContext): Unit = {
        if (finalData != null) {
            var write: DataFrame = null
            val finalData1 = finalData  //.sort("id").dropDuplicates(Seq("id"))
            Common.logger("FinalData not null, data will be write in: " + outputPath)
            val parquetField = getParquetField(fieldsSelected)
            write = convertToParquetType(finalData1, parquetField)
            var b = Try {
                write.coalesce(1).write.mode(writeMode).format(writeFormat).save(outputPath)
            }
            if (b.isSuccess) {
                Common.logger("Write done")
            } else {
                writeSchemaWithoutData(sc, sqlContext)
            }
        } else {
            writeSchemaWithoutData(sc, sqlContext)
        }
    }

}
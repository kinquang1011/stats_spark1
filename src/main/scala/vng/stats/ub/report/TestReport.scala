package vng.stats.ub.report

import scala.reflect.runtime.universe
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import vng.stats.ub.utils.Constants
import vng.stats.ub.utils.DataUtils
import java.util.Date
import vng.stats.ub.utils.DateTimeUtils
import org.apache.spark.sql.DataFrame

abstract class TestReport[T <: Product] {

    val sc: SparkContext = {

        val conf = new SparkConf().setAppName("")
        conf.set("spark.hadoop.validateOutputSpecs", "false")

        new SparkContext(conf)
    }
    
    val sqlContext: SQLContext =  {

        val sqlContext = new SQLContext(sc)
        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
        sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
        import sqlContext.implicits._
        
        sqlContext
    }
    
    private var parameters: Map[String, String] = _

    var gameCode = ""
    var logDate = ""
    var timing = ""
    var calcId = ""
    var source = ""
    var inputPath = ""
    var outputPath = ""
    
    var groupId = ""
    
    var createDate = ""
    var reportDate = ""
    var output: List[T] = Nil
    
    final def init(args: Array[String]): Unit = {
        
        parameters = DataUtils.getParameters(args)
        readDefaultParams()
        readExtraParams()
    }

    final def readDefaultParams(): Unit = {
        
        gameCode = parameters.get(Constants.Parameters.GAME_CODE).get
        logDate = parameters.get(Constants.Parameters.LOG_DATE).get
        timing = parameters.get(Constants.Parameters.TIMING).get
        calcId = parameters.get(Constants.Parameters.CALC_ID).get
        source = parameters.get(Constants.Parameters.SOURCE).get
        inputPath = parameters.get(Constants.Parameters.INPUT_PATH).get
        outputPath = parameters.get(Constants.Parameters.OUTPUT_PATH).get
        
        groupId = parameters(Constants.Parameters.GROUP_ID)
        
        createDate = DateTimeUtils.getDateString(new Date())
        reportDate = DataUtils.formatReportDate(logDate)
        
        output = List[T]()
    }
    
    def readExtraParams(): Unit = {
        
    }

    def excute(): Unit

    def write(): Unit = {

        /*import sqlContext.implicits._
        var x = sc.parallelize(output, 1)
        var df = sqlContext.createDataFrame(x)
        df.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", "\t").save(outputPath + "/" + logDate)*/
    }

    final def run(args: Array[String]): Unit = {

        init(args)
        excute()
        write()
        destroy()
    }
    
    final def destroy() {
        sc.stop()
    }
    
    def main(args: Array[String]) {

        run(args)
    }
}
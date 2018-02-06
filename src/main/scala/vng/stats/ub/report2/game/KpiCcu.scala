package vng.stats.ub.report2.game

import vng.stats.ub.utils.DataUtils
import vng.stats.ub.report2.BaseReport
import org.apache.spark.sql.DataFrame
import vng.stats.ub.utils.Common
import org.apache.spark.sql.SQLContext
import vng.stats.ub.common.KpiFormat
import vng.stats.ub.utils.Constants
import org.apache.spark.sql.functions._
import vng.stats.ub.sql.report.MysqlGameReport

object KpiCcu extends BaseReport {

    var ccuPath = ""
    
    override def readExtraParams(): Unit = {
        
        ccuPath = parameters(Constants.Parameters.CCU_PATH)
    }
    
    def excute(sqlContext: SQLContext): DataFrame = {
        
        if(timing != "a1"){
            
            sqlContext.emptyDataFrame
        }
        
        var lstLogFiles = DataUtils.getListFiles(ccuPath, logDate, timing);
        var logDF = sqlContext.read.option("mergeSchema", "true").parquet(lstLogFiles: _*)

        var ccuDF = logDF.select("log_date", "ccu").groupBy("log_date").agg(sum("ccu").as('ccu)).agg(avg("ccu"), max("ccu"))
        
        var output = List[KpiFormat]()
        
        ccuDF.collect().foreach { row =>
            var avg = row.getDouble(0)
            var max = row.getLong(1)

            output = KpiFormat(source, gameCode, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.ACU, timing), avg) :: output
            output = KpiFormat(source, gameCode, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.PCU, timing), max) :: output
            
            Common.logger("Game: " + gameCode + ", avg: " + avg + ", max: " + max)
        }
        
        MysqlGameReport.insert(output, calcId)
        
        var x = sc.parallelize(output, 1)
        var df = sqlContext.createDataFrame(x)
        
        df
    }
}
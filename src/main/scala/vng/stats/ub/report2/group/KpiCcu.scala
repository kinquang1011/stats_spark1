package vng.stats.ub.report2.group

import vng.stats.ub.utils.DataUtils
import vng.stats.ub.report2.BaseReport
import org.apache.spark.sql.DataFrame
import vng.stats.ub.utils.Common
import org.apache.spark.sql.SQLContext
import vng.stats.ub.common.KpiFormat
import vng.stats.ub.utils.Constants
import org.apache.spark.sql.functions._
import vng.stats.ub.common.KpiGroupFormat
import vng.stats.ub.sql.report.MysqlGroupReport

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
        var logDF = sqlContext.read.parquet(lstLogFiles: _*)

        var ccuDF = logDF.select("log_date", groupId, "ccu").groupBy("log_date", groupId).agg(sum("ccu").as('ccu)).groupBy(groupId).agg(avg("ccu"), max("ccu"))
        
        var output = List[KpiGroupFormat]()
        
        ccuDF.collect().foreach { row =>
            var groupId = row.getString(0)
            var avg = row.getDouble(1)
            var max = row.getLong(2)

            output = KpiGroupFormat(source, gameCode, groupId, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.ACU, timing), avg) :: output
            output = KpiGroupFormat(source, gameCode, groupId, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.PCU, timing), max) :: output
            
            Common.logger("Group: " + groupId + ", avg: " + avg + ", max: " + max)
        }
        
        MysqlGroupReport.insert(output, groupId, calcId)
        
        var x = sc.parallelize(output, 1)
        var df = sqlContext.createDataFrame(x)
        
        df
    }
}
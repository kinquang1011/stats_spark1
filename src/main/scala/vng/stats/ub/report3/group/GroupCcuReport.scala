package vng.stats.ub.report3.group

import vng.stats.ub.sql.report.MysqlGroupReport
import vng.stats.ub.common.KpiGroupFormat
import org.apache.spark.sql.DataFrame
import vng.stats.ub.report3.BaseReport
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.utils.Common
import vng.stats.ub.utils.Constants
import org.apache.spark.sql.functions._

object GroupCcuReport extends BaseReport {

    def excute(dataframe: DataFrame): DataFrame = {
        
        var resultDF = dataframe.select(groupId, "ccu")
        .withColumn(groupId, makeOtherIfNull(dataframe(groupId)))
        .groupBy(groupId).agg(avg("ccu"), max("ccu"))
        
        resultDF
    }
    
    def store(dataframe: DataFrame): Unit = {
        
        var output = List[KpiGroupFormat]()
        
        dataframe.collect().foreach { row =>
            
            var groupId = row.getString(0)
            var avg = row.getDouble(1)
            var max = row.getLong(2)

            output = KpiGroupFormat(source, gameCode, groupId, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.ACU, timing), avg) :: output
            output = KpiGroupFormat(source, gameCode, groupId, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.PCU, timing), max) :: output
            
            Common.logger("Group: " + groupId + ", avg: " + avg + ", max: " + max)
        }
        
        if(groupId == "sid" || groupId == "country" || groupId == "os"){
            
            MysqlGroupReport.insertJson(output, groupId, calcId)
        } else {
            MysqlGroupReport.insert(output, groupId, calcId)
        }
    }
}
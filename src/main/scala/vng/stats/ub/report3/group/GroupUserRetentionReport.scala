package vng.stats.ub.report3.group

import vng.stats.ub.common.KpiGroupFormat
import org.apache.spark.sql.DataFrame
import vng.stats.ub.report3.BaseReport
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.utils.Common
import vng.stats.ub.utils.Constants
import org.apache.spark.sql.functions._
import vng.stats.ub.sql.report.MysqlGroupReport

object GroupUserRetentionReport extends BaseReport {

    def excute(dataframe: DataFrame): DataFrame = {
        
        var resultDF = dataframe.select(groupId, "pa" + calcId, "ca" + calcId)
        .withColumn(groupId, makeOtherIfNull(dataframe(groupId)))
        .groupBy(groupId).agg(count("pa" + calcId), count("ca" + calcId))
        
        resultDF
    }
    
    def store(dataframe: DataFrame): Unit = {
        
        var output = List[KpiGroupFormat]()
        
        dataframe.collect().foreach { row =>
                
            var rate = 0.0
            var groupId = row.getString(0)
            var totalAcc = row.getLong(1)
            var retention = row.getLong(2)
            
            if (totalAcc != 0) {
                
                rate = retention * 100.0 / totalAcc
            }

            output = KpiGroupFormat(source, gameCode, groupId, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.RETENTION_PLAYING_RATE, timing), rate) :: output
            
            Common.logger("groupId: " + groupId + ", total: " + totalAcc + ", retention: " + retention)
        }
        
        if(groupId == "sid" || groupId == "country" || groupId == "os"){
            
            MysqlGroupReport.insertJson(output, groupId, calcId)
        } else {
            MysqlGroupReport.insert(output, groupId, calcId)
        }
    }
}
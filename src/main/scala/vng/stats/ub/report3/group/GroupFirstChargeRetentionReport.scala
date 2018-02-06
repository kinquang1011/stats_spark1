package vng.stats.ub.report3.group

import vng.stats.ub.sql.report.MysqlGroupReport
import vng.stats.ub.common.KpiGroupFormat
import org.apache.spark.sql.DataFrame
import vng.stats.ub.report3.BaseReport
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.utils.Common
import vng.stats.ub.utils.Constants
import org.apache.spark.sql.functions._
import vng.stats.ub.sql.report.MysqlGroupReport

object GroupFirstChargeRetentionReport extends BaseReport {

    def excute(dataframe: DataFrame): DataFrame = {
        
        var resultDF = dataframe.select(groupId, "pp" + calcId, "cp" + calcId)
        .withColumn(groupId, makeOtherIfNull(dataframe(groupId)))
        .groupBy(groupId).agg(count("pp" + calcId), count("cp" + calcId))
        
        resultDF
    }
    
    def store(dataframe: DataFrame): Unit = {
        
        var output = List[KpiGroupFormat]()
        
        dataframe.collect().foreach { row =>
                
            var rate = 0.0
            var groupId = row.getString(0)
            var totalFirst = row.getLong(1)
            var totalRetention = row.getLong(2)

            if (totalFirst != 0) {
                rate = totalRetention * 100.0 / totalFirst
            }
            output = KpiGroupFormat(source, gameCode, groupId, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.RETENTION_PAYING_RATE, timing), rate) :: output
            Common.logger("Game: " + gameCode + ", groupId: " + groupId +  ", first charge: " + totalFirst + ", retention: " + totalRetention)
        }
        
        if(groupId == "sid" || groupId == "country" || groupId == "os"){
            
            MysqlGroupReport.insertJson(output, groupId, calcId)
        } else {
            MysqlGroupReport.insert(output, groupId, calcId)
        }
    }
}
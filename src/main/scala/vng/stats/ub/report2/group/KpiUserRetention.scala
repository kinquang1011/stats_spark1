package vng.stats.ub.report2.group

import scala.reflect.runtime.universe
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.count
import vng.stats.ub.common.KpiGroupFormat
import vng.stats.ub.common.Schemas
import vng.stats.ub.report2.BaseReport
import vng.stats.ub.utils.Common
import vng.stats.ub.utils.Constants
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.sql.report.MysqlGroupReport

object KpiUserRetention extends BaseReport {

    var activityPath = ""
    
    override def readExtraParams(): Unit = {
        
        activityPath = parameters(Constants.Parameters.ACTIVITY_PATH)
    }
    
    def excute(sqlContext: SQLContext): DataFrame = {

        var activityFile = DataUtils.getFile(activityPath, logDate);
        var prevActivityFile = DataUtils.getFile(activityPath, logDate, timing);
        
        var loginDF = sqlContext.read.parquet(activityFile)
        
        var userDF = loginDF.select(groupId, calcId).distinct()
        var prevDF = sqlContext.createDataFrame(sc.emptyRDD[Row], Schemas.Activity)
        var output = List[KpiGroupFormat]()
        
        if (!DataUtils.isEmpty(prevActivityFile)) {
    
            prevDF = sqlContext.read.parquet(prevActivityFile).select(groupId, calcId).distinct
            var joinDF = prevDF.as('p).join(userDF.as('u), prevDF(calcId) === userDF(calcId) && prevDF(groupId) === userDF(groupId), "left_outer")
            var resultDF = joinDF.groupBy("p." + groupId).agg(count("p." + calcId), count("u." + calcId))
            
            resultDF.collect().foreach { row =>
                
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
        }

        MysqlGroupReport.insert(output, groupId, calcId)
        
        var x = sc.parallelize(output, 1)
        var df = sqlContext.createDataFrame(x)
        
        df
    }
}
package vng.stats.ub.report2.game

import scala.reflect.runtime.universe
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import vng.stats.ub.common.KpiFormat
import vng.stats.ub.common.Schemas
import vng.stats.ub.report2.BaseReport
import vng.stats.ub.utils.Common
import vng.stats.ub.utils.Constants
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.sql.report.MysqlGameReport

object KpiUserRetention extends BaseReport {

    var activityPath = ""
    
    override def readExtraParams(): Unit = {
        
        activityPath = parameters(Constants.Parameters.ACTIVITY_PATH)
    }
    
    def excute(sqlContext: SQLContext): DataFrame = {

        var activityFile = DataUtils.getFile(activityPath, logDate);
        var prevActivityFile = DataUtils.getFile(activityPath, logDate, timing);

        var loginDF = sqlContext.read.option("mergeSchema", "true").parquet(activityFile)

        var userDF = loginDF.select(calcId).coalesce(1).distinct()
        var prevDF = sqlContext.createDataFrame(sc.emptyRDD[Row], Schemas.Activity)
        var output = List[KpiFormat]()
        var rate = 0.0

        if (!DataUtils.isEmpty(prevActivityFile)) {

            prevDF = sqlContext.read.option("mergeSchema", "true").parquet(prevActivityFile).select(calcId).coalesce(1).distinct
            var joinDF = prevDF.as('p).join(userDF.as('u), prevDF(calcId) === userDF(calcId), "leftsemi")
            var retention = joinDF.select(calcId).distinct().count
            var total = prevDF.select(calcId).distinct().count

            if(total != 0){
                
                rate = retention * 100.0 / total
            }
            output = KpiFormat(source, gameCode, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.RETENTION_PLAYING_RATE, timing), rate) :: output

            Common.logger("Game: " + gameCode + ", total: " + total + ", retention: " + retention)
        }
        
        MysqlGameReport.insert(output, calcId)

        var x = sc.parallelize(output, 1)
        var df = sqlContext.createDataFrame(x)

        df
    }
}
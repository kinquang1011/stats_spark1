package vng.stats.ub.report2.game

import vng.stats.ub.utils.DataUtils
import vng.stats.ub.sql.report.MysqlGameReport
import org.apache.spark.sql.DataFrame
import vng.stats.ub.utils.Common
import org.apache.spark.sql.SQLContext
import vng.stats.ub.common.KpiFormat
import vng.stats.ub.utils.Constants
import vng.stats.ub.report2.BaseReport
import org.apache.spark.sql.functions.sum

object GameUserPlayingTime extends BaseReport {

    var activityPath = ""
    
    override def readExtraParams(): Unit = {
        
        activityPath = parameters(Constants.Parameters.ACTIVITY_PATH)
    }
    
    def excute(sqlContext: SQLContext): DataFrame = {
        
        var lstLogFiles = DataUtils.getListFiles(activityPath, logDate, timing);
        var logDF = sqlContext.read.option("mergeSchema", "true").parquet(lstLogFiles: _*)

        var playingtimeDF = logDF.where("action = 'logout'").agg(sum("online_time"))
        var total = playingtimeDF.first().getAs[Long](0)
        
        var output = List[KpiFormat]()
        var id = DataUtils.getKpiId(calcId, Constants.Kpi.PLAYING_TIME, timing)
        output = KpiFormat(source, gameCode, reportDate, createDate, id, total) :: output
        
        Common.logger("Total: " + total + ", id: " + id)
        
        MysqlGameReport.insert(output, calcId)
        
        var x = sc.parallelize(output, 1)
        var df = sqlContext.createDataFrame(x)
        
        df
    }
}
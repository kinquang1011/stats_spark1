package vng.stats.ub.report2.group

import vng.stats.ub.report2.BaseReport
import vng.stats.ub.common.KpiGroupFormat
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.common.Schemas
import org.apache.spark.sql.Row
import vng.stats.ub.utils.DateTimeUtils
import java.util.Date
import org.apache.spark.sql.functions._
import vng.stats.ub.utils.Constants
import vng.stats.ub.utils.Common
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import vng.stats.ub.sql.report.MysqlGroupReport

object KpiAccountRegister extends BaseReport {

    var accregisterPath = ""
    
    override def readExtraParams(): Unit = {
        
        accregisterPath = parameters(Constants.Parameters.ACC_REGISTER_PATH)
    }
    
    def excute(sqlContext: SQLContext): DataFrame = {

        var accRegDF = sqlContext.createDataFrame(sc.emptyRDD[Row], Schemas.AccountRegister)
        var lstAccRegPath = DataUtils.getListFiles(accregisterPath, logDate, timing);

        if (!DataUtils.isEmpty(lstAccRegPath)) {

            accRegDF = sqlContext.read.parquet(lstAccRegPath: _*)
        }
        
        var resultDF = accRegDF.select(groupId, calcId).distinct.groupBy(groupId).agg(count(calcId))
        var output = List[KpiGroupFormat]()
        
        resultDF.collect().foreach { row =>
            var groupId = row.getString(0)
            var totalReg = row.getLong(1)

            var id = DataUtils.getKpiId(calcId, Constants.Kpi.NEW_ACCOUNT_PLAYING, timing)
            output = KpiGroupFormat(source, gameCode, groupId, reportDate, createDate, id, totalReg) :: output
            
            Common.logger("groupId: " + groupId + ", Total: " + totalReg + ", id: " + id)
        }
        
        MysqlGroupReport.insert(output, groupId, calcId)
        
        var x = sc.parallelize(output, 1)
        var df = sqlContext.createDataFrame(x)
        
        df
    }
}
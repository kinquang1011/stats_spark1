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

object KpiNewUserRetention extends BaseReport {
  
    var activityPath = ""
    var accregisterPath = ""
    
    override def readExtraParams(): Unit = {
        
        accregisterPath = parameters(Constants.Parameters.ACC_REGISTER_PATH)
        activityPath = parameters(Constants.Parameters.ACTIVITY_PATH)
    }
    
    def excute(sqlContext: SQLContext): DataFrame = {

        var loginFile = DataUtils.getFile(activityPath, logDate);
        var accRegisterFile = DataUtils.getFile(accregisterPath, logDate, timing);
        
        var loginDF = sqlContext.read.parquet(loginFile)
        
        var userDF = loginDF.select(groupId, calcId).distinct()
        var newAccDF = sqlContext.createDataFrame(sc.emptyRDD[Row], Schemas.AccountRegister)
        var output = List[KpiGroupFormat]()
        
        if (!DataUtils.isEmpty(accRegisterFile)) {
    
            newAccDF = sqlContext.read.parquet(accRegisterFile).select(groupId, calcId)
            var joinDF = newAccDF.as('n).join(userDF.as('u), newAccDF(calcId) === userDF(calcId) && newAccDF(groupId) === userDF(groupId), "left_outer")
            var resultDF = joinDF.groupBy("n." + groupId).agg(count("n." + calcId), count("u." + calcId))
            
            resultDF.collect().foreach { row =>
                
                var rate = 0.0
                var groupId = row.getString(0)
                var newAcc = row.getLong(1)
                var retention = row.getLong(2)
                
                if(newAcc != 0) {
                    rate = retention * 100.0 / newAcc
                }
    
                output = KpiGroupFormat(source, gameCode, groupId, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NEW_USER_RETENTION_RATE, timing), rate) :: output
                
                Common.logger("groupId: " + groupId + ", new: " + newAcc + ", retention: " + retention)
            }
        }
        
        MysqlGroupReport.insert(output, groupId, calcId)

        var x = sc.parallelize(output, 1)
        var df = sqlContext.createDataFrame(x)
        
        df
    }
}
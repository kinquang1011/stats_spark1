package vng.stats.ub.report2.game

import vng.stats.ub.utils.DataUtils
import vng.stats.ub.sql.report.MysqlGameReport
import org.apache.spark.sql.DataFrame
import vng.stats.ub.utils.Common
import org.apache.spark.sql.SQLContext
import vng.stats.ub.common.KpiFormat
import vng.stats.ub.utils.Constants
import vng.stats.ub.report2.BaseReport
import vng.stats.ub.common.Schemas
import org.apache.spark.sql.Row
import vng.stats.ub.utils.DateTimeUtils
import org.apache.spark.sql.functions._
import net.liftweb.json.JsonAST._
import net.liftweb.json.Extraction._
import net.liftweb.json.Printer._
import vng.stats.ub.common.KpiGameRetentionFormat
import vng.stats.ub.sql.report.MysqlGameRetentionReport

object GameRetention extends BaseReport {

    var activityPath = ""
    var totalLoginPath = ""
    
    override def readExtraParams(): Unit = {
        
        totalLoginPath = parameters(Constants.Parameters.TOTAL_LOGIN_PATH)
        activityPath = parameters(Constants.Parameters.ACTIVITY_PATH)
    }
    
    var periodUDF = udf { (logDate: String, timing: String) => 
        DateTimeUtils.getTimePeriod(timing, logDate)
    }

    def excute(sqlContext: SQLContext): DataFrame = {

        var lstLoginFiles = DataUtils.getListFiles(activityPath, logDate, timing)
        var totalAccFile = DataUtils.getFile(totalLoginPath, logDate)

        var userDF = sqlContext.read.option("mergeSchema", "true").parquet(lstLoginFiles:_*)
        userDF = userDF.select(periodUDF(col("log_date"), lit(timing)).as('log_date), col(calcId)).distinct()
        
        var totalAccDF = sqlContext.read.option("mergeSchema", "true").parquet(totalAccFile).where("log_date <= '" + logDate + " 24:00:00'")
        totalAccDF = totalAccDF.select(periodUDF(col("log_date"), lit(timing)).as('log_date), col(calcId))
        
        var retentionDF = userDF.as('u).join(totalAccDF.as('t), userDF(calcId) === totalAccDF(calcId)).selectExpr("t.log_date as reg_date", "u.log_date", "u." + calcId)
        var resultDF = retentionDF.groupBy("reg_date", "log_date").agg(count(calcId).as('retention))
        
        var jsonObj = Map[String, Any]()
        implicit val formats = net.liftweb.json.DefaultFormats
        
        resultDF.collect().foreach { row => {
            
                var regDate = row.getAs[String]("reg_date")
                var retention = row.getAs[Long]("retention")
                
                jsonObj += (regDate -> retention)
            }
        }
        
        var json = compact(render(decompose(jsonObj)))
        
        var output = List[KpiGameRetentionFormat]()
        var id = DataUtils.getKpiId(calcId, Constants.Kpi.USER_RETENTION, timing)
        output = KpiGameRetentionFormat(source, gameCode, reportDate, createDate, id, json) :: output
        
        Common.logger("Retention: " + json + ", id: " + id)
        
        MysqlGameRetentionReport.insert(output, calcId)
        
        var x = sc.parallelize(output, 1)
        var df = sqlContext.createDataFrame(x)
        
        df
    }
}
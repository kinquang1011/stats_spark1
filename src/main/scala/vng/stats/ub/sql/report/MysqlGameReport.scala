package vng.stats.ub.sql.report

import vng.stats.ub.sql.DbMySql
import vng.stats.ub.common.KpiFormat
import scala.collection.mutable.MutableList
import scala.collection.mutable.Map
import java.util.HashMap
import vng.stats.ub.utils.Common
import scala.collection.mutable.LinkedHashMap


object MysqlGameReport {

    object CALCID {
        
        val ID = "id"
        val DID = "did"
    }
    
    var mysql: DbMySql = new DbMySql()

    var table = ""
    
    def getTableName(calcId: String): String = {
        
        calcId match {
            case CALCID.ID => {
                
                "game_kpi"
            }
            case CALCID.DID => {
                
                "device_id_kpi"
            }
        }
    }
    
    def insert(lstOutput: List[KpiFormat], calcId: String): Unit = {

        delete(lstOutput, calcId)
        
        var results = List[LinkedHashMap[String, Any]]()

        for (values <- lstOutput) {

            var row = LinkedHashMap[String, Any]()

            row += ("source" -> values.source)
            row += ("gameCode" -> values.gameCode)
            row += ("logDate" -> values.logDate)
            row += ("createDate" -> values.createDate)
            row += ("kpiId" -> values.kpiId)
            row += ("value" -> values.value)

            results ++= List(row)
        }

        table = getTableName(calcId)
        Common.logger("insert into " + table)
        
        var sql = "insert into " + table + " (source, game_code, report_date, calc_date, kpi_id, kpi_value) values (?, ?, ?, ?, ?, ?)"
        mysql.excuteBatchInsert(sql, results)
    }
    
    def delete(lstOutput: List[KpiFormat], calcId: String): Unit = {

        var results = List[LinkedHashMap[String, Any]]()

        for (values <- lstOutput) {

            var row = LinkedHashMap[String, Any]()
            
            row += ("source" -> values.source)
            row += ("gameCode" -> values.gameCode)
            row += ("logDate" -> values.logDate)
            row += ("kpiId" -> values.kpiId)

            results ++= List(row)
        }

        table = getTableName(calcId)
        Common.logger("delete from " + table)
        
        var sql = "delete from " + table + " where source = ? and game_code = ? and report_date = ? and kpi_id = ?"
        mysql.excuteBatchDelete(sql, results)
    }
}
package vng.stats.ub.tezt

import scala.collection.mutable.LinkedHashMap
import net.liftweb.json.JsonAST._
import net.liftweb.json.Extraction._
import net.liftweb.json.Printer._
import vng.stats.ub.common.KpiGroupFormat
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.utils.Constants

object Child extends Parent("My Name"){
  
    var d = '\0'
    
  def main(args: Array[String]){
    
        /*var results = List[LinkedHashMap[String, Any]]()
        var jsonObj = LinkedHashMap[String, Any]()
        
        var logDate = ""
        var gameCode = ""
        var source = ""
        var kpiId = 0
        var createDate = ""

        var row = Map[String, Any]()

        logDate = "12"
        gameCode = "12"
        source = "31"
        kpiId = 1001
        createDate = "23"
        
        row += ("s1" -> 15)

        implicit val formats = net.liftweb.json.DefaultFormats
        var json = compact(render(decompose(row)))
        println(json)
        
        var mp = LinkedHashMap("logDate" -> logDate, "gameCode" -> gameCode, "source" -> source, "kpiId" -> kpiId, "value" -> json, "createDate" -> createDate)
        results ++= List(mp)*/
        
         var output = List[KpiGroupFormat]()
         output = KpiGroupFormat("ingame", "contra", "S1", "2016-10-10", "2016-10-20", 10001, 10) :: output
         output = KpiGroupFormat("ingame", "contra", "S2", "2016-10-10", "2016-10-20", 10001, 10) :: output
         output = KpiGroupFormat("ingame", "contra", "S2", "2016-10-10", "2016-10-20", 20001, 20) :: output
         output = KpiGroupFormat("ingame", "contra", "S1", "2016-10-10", "2016-10-20", 11001, 11) :: output
         output = KpiGroupFormat("ingame", "contra", "S2", "2016-10-10", "2016-10-20", 30001, 30) :: output
         output = KpiGroupFormat("ingame", "contra", "S1", "2016-10-10", "2016-10-20", 15001, 15) :: output
         
         var params = output.groupBy(row => row.kpiId).map(k => (k._1, k._2.map { x => (x.groupId, x.value) })).map(row => (row._1, row._2.map(x => x._1)))
         println(params)
         
         /*var results = List[LinkedHashMap[String, Any]]()
        var jsonObj = Map[String, Any]()
        
        var logDate = ""
        var gameCode = ""
        var source = ""
        var kpiId = 0
        var createDate = ""

        for (values <- output) {

            logDate = values.logDate
            gameCode = values.gameCode
            source = values.source
            kpiId = values.kpiId
            createDate = values.createDate

            jsonObj += (values.groupId -> values.value)
        }
    
         
         implicit val formats = net.liftweb.json.DefaultFormats
        var json = compact(render(decompose(jsonObj)))
        println(json)*/
  }
}
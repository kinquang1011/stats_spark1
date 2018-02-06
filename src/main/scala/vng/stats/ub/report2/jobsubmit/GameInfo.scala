package vng.stats.ub.report2.jobsubmit

import scala.collection.immutable.TreeMap

object GameInfo {
    
    var GAMES = TreeMap(
        "3qmobile" -> "2016-05-05",
        "bln" -> "2016-03-02",
        "contra" -> "2015-10-15",
        "dptk" -> "2016-01-06",
        "dttk" -> "2014-12-25",
        "htc" -> "2016-03-29",
        "kftl" -> "2016-04-21",
        "nikki" -> "2016-02-21",
        "pmcl" -> "2015-11-02",
        "pv3d" -> "2016-05-24",
        "stonyvn" -> "2016-05-25",
        "tlbbm" -> "2015-06-26",
        "tths" -> "2016-01-20",
        "tvl" -> "2015-12-20",
        "wefight" -> "2015-12-08",
        
        "cack" -> "2016-06-22",
        "bklr" -> "2016-08-03",
        "stct" -> "2016-06-22",
        
        "ts" -> "2016-06-22",
        "kv" -> "2016-05-25",
        "dcc" -> "2016-06-22",
        "ck" -> "2016-04-28",
        "g10sea" -> "2016-07-01",
        "stonysea" -> "2016-07-20",
        
        "naruto" -> "2016-07-14",
        "tfzfbs2" -> "2015-07-22",
        "coccm" -> "2016-07-03",
        "ctpgsn" -> "2016-04-21",
        "tttd" -> "2016-08-11",
        "cfgfbs1" -> "2016-04-22"
    )
    
    def main(args: Array[String]) {
        
        for((k, v) <- GAMES.toSeq.sortBy(_._2)) {
            println(v + ": \t" + k)
        }
    }
}
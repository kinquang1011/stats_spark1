package vng.stats.ub.normalizer.format.v2

import scala.collection.immutable.HashMap

/**
 * Created by tuonglv on 25/10/2016.
 */
class HiveQueryConfig  extends  Serializable {
    var _select: String = ""
    var _from: String = ""
    var _where: String = ""
    var _mapping:Array[String] = Array()

    var _udfRegister: Map [ String, (Seq[Any]) => String] = HashMap()

    def registUdf(funcName: String, _func: (Seq[Any]) => String): Unit = {
        _udfRegister += (funcName -> _func)
    }

    def select(_s: String, _alias: String = ""): Unit = {
        if(_select == ""){
            _select = _s
            if(_alias != ""){
                _select = _select + " as " + _alias
            }
        }else{
            _select = _select + ", " + _s
            if(_alias != ""){
                _select = _select + " as " + _alias
            }
        }

        if(_alias != ""){
            _mapping = _mapping ++ Array(_alias)
        }else{
            _mapping = _mapping ++ Array(_s)
        }
    }

    def from(_f: String): Unit = {
        _from = _f
    }

    def where(_fi: String, _va: String = ""): Unit = {
        var _w: String = _fi
        if(_va != ""){
            _w = _fi + " = '" + _va + "'"
        }
        if(_where == ""){
            _where =  _w + " "
        }else{
            _where = _where + " and " + _w + " "
        }
    }
    def where_in(_fi: String, pa: Array[String]): Unit ={
        var wis = combineWhereIn(pa)
        if(_where == ""){
            _where = _fi + " in " + wis
        }else{
            _where = _where + " and " + _fi + " in " + wis
        }
    }

    def where_or(): Unit ={

    }

    def build_query(): String ={
        var sql: String = "select "
        sql = sql + _select
        sql = sql + " from " + _from
        sql = sql + " where " + _where
        sql
    }
    def getMapping(): Array[String] = {
        _mapping
    }
    private def combineWhereIn(wi: Array[String]): String = {
        var rs = "("
        wi.foreach { pa =>
            rs = rs + "'" + pa + "',"
        }
        rs = rs.dropRight(1)
        rs = rs + ")"
        rs
    }
}

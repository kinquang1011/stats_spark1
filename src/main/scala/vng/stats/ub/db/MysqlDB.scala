package vng.stats.ub.db


import java.sql.{ResultSet, DriverManager, Connection}

import vng.stats.ub.utils.Common

/**
 * Created by tuonglv on 27/06/2016.
 */
class MysqlDB extends Serializable {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://10.60.22.2/ubstats"
    val username = "ubstats"
    val password = "pubstats"

    var isOpen = false
    // there's probably a better way to do this
    var connection: Connection = null
    connect()
    def connect(): Unit ={
        try {
            // make the connection
            Class.forName(driver)
            connection = DriverManager.getConnection(url, username, password)
            isOpen = true
        } catch {
            case e => e.printStackTrace
                println("MysqlDB connect exception")
        }
    }
    def close(): Unit ={
        isOpen = false
        connection.close()
    }

    def executeQuery (sql: String, thenClose: Boolean = true): ResultSet ={
        Common.logger("Execute sql = " + sql)
        var rs: ResultSet = null
        try{
            var stmt = connection.prepareStatement(sql)
            rs = stmt.executeQuery()
            //val statement = connection.createStatement()
            //statement.execute(sql)
        }catch  {
            case e => e.printStackTrace
                println("MysqlDB insert exception")
        }
        if(thenClose == true){
            close()
        }
        rs
    }
    def executeUpdate (sql: String, thenClose: Boolean = true): Int ={
        Common.logger("Execute sql = " + sql)
        var rs: Int = 0
        try{
            var stmt = connection.prepareStatement(sql)
            rs = stmt.executeUpdate()
        }catch  {
            case e => e.printStackTrace
                println("MysqlDB insert exception")
        }
        if(thenClose == true){
            close()
        }
        rs
    }

    def deleteRecord(whereMap:Map[String, String], tableName: String): Unit ={
        var where = ""
        whereMap.keys.foreach { key =>
            var values = whereMap(key).toString
            where = where + key.toString + "='" + values + "' and "
        }
        where = where.dropRight(5)
        var sql = "delete from " + tableName + " where " + where
        executeUpdate(sql, false)
    }

    def selectRecord(fields:Array[String], whereMap:Map[String, String], tableName: String): ResultSet = {
        var fieldSelect = ""
        for (field <- fields) {
            fieldSelect += field + ","
        }
        fieldSelect = fieldSelect.dropRight(1)

        var where = ""
        whereMap.keys.foreach { key =>
            var values = whereMap(key).toString
            where = where + key.toString + "='" + values + "' and "
        }
        where = where.dropRight(5)
        var sql = "select " + fieldSelect + " from " + tableName + " where " + where
        var rs: ResultSet = executeQuery(sql, false)
        //Common.logger("Fetch Size = " + rs.getRow + ", sql = " + sql)
        rs
    }

    def insertOrUpdate(deleteMap:Map[String, String], dataMap:Map[String, String], tableName: String): Unit = {
        var rs: ResultSet = selectRecord(Array("*"), deleteMap, tableName)

        var size:Int = 0
        if(rs != null && rs.last()){
            size = rs.getRow()
        }

        if(rs == null || size == 0){
            insertRecord(dataMap, tableName)
        }else if (size == 1) {
            deleteRecord(deleteMap, tableName)
            insertRecord(dataMap, tableName)
        } else {
            Common.logger("Many lines are exist, num = " + size)
        }
    }
    def insertRecord(dataMap:Map[String, String], tableName: String): Unit ={

        var keysString = ""
        dataMap.keys.foreach { key =>
            keysString = keysString + key + ","
        }
        keysString = keysString.dropRight(1)
        var valuesString = ""
        dataMap.values.foreach { value =>
            valuesString = valuesString + "'"  + value + "',"
        }
        valuesString = valuesString.dropRight(1)
        var sql = "insert into " + tableName + "(" + keysString + ") values("  + valuesString + ")"
        executeUpdate(sql, false)
    }
}

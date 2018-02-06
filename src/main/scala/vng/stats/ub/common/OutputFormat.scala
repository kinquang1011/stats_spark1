package vng.stats.ub.common

case class OutputFormat(gameCode: String, logDate: String, createDate: String, key: String, value: Long)
case class OutputDoubleFormat(gameCode: String, logDate: String, createDate: String, key: String, value: Double)
case class KpiFormat(source: String, gameCode: String, logDate: String, createDate: String, kpiId: Integer, value: Double)
case class KpiGroupFormat(source: String, gameCode: String, groupId: String, logDate: String, createDate: String, kpiId: Integer, value: Double)
case class KpiGameRetentionFormat(source: String, gameCode: String, logDate: String, createDate: String, kpiId: Integer, value: String)
package vng.stats.ub.normalizer.format.v2

import org.apache.spark.sql.DataFrame

import scala.collection.immutable.HashMap

/**
 * Created by tuonglv on 25/10/2016.
 */
class JsonFormatConfig  extends  Serializable {
    var filter: (DataFrame) => DataFrame = null
    var generate: (DataFrame) => DataFrame = null
}

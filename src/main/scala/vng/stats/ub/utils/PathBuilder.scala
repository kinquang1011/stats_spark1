package vng.stats.ub.utils

/**
 * Created by tuonglv on 17/11/2016.
 */
class PathBuilder {
    // daily or hourly
    private var _timing = ""

    val INGAME_WAREHOUSE = "/ge/warehouse"
    val DAILY_DS_FORMAT = "yyyy-MM-dd"
    val HOURLY_DS_FORMAT = "yyyyMMdd"

    def setTiming(timing: String): PathBuilder = {
        _timing = timing
        this
    }
    def getTiming(): String = {
        _timing
    }



}

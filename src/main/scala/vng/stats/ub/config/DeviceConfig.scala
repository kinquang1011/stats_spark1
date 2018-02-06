package vng.stats.ub.config

/**
 * Created by tuonglv on 10/05/2016.
 */

object DeviceConfig extends Serializable{
    val defaultGameCode = "default_game"
    val myOsMap = Map(
        "(android){1}" -> "android",
        "(ios){1}" -> "ios",
        "(android){1} (\\d+).(\\d+)" -> "android{2}.{3}",
        "(iphone os){1} (\\d+).(\\d+)" -> "ios{2}.{3}",
        "(ios){1} (\\d+).(\\d+)" -> "ios{2}.{3}"
    )

    val myNwMap = Map(
        "2g" -> "2g",
        "3g" -> "3g",
        "wap" -> "wap",
        "wifi" -> "wifi",
        "wwan" -> "wwan"
    )

    val myDtyMap = Map (
        "asus" -> "asus",
        "fpt" -> "fpt",
        "galaxy" -> "galaxy",
        "gt-" -> "gt",
        "hkphone" -> "hkphone",
        "htc" -> "htc",
        "huawei" -> "huawei",
        "ipad" -> "ipad",
        "iphone" -> "iphone",
        "lenovo" -> "lenovo",
        "lg-" -> "lg",
        "sm-" -> "sm",
        "nexus" -> "nexus",
        "masstel" -> "masstel",
        "mobiistar" -> "mobiistar",
        "nexus" -> "nexus",
        "nokia" -> "nokia",
        "q-smart" -> "q-smart",
        "redmi" -> "redmi",
        "venue" -> "venue",
        "samsung" -> "samsung",
        "shv-" -> "samsung",
        "sgh-" -> "samsung",
        "viettel" -> "viettel"
    )
    val myTelMap = Map (
        "viettel" -> "viettel",
        "carrier" -> "carrier",
        "mobifone" -> "mobifone",
        "vinaphone" -> "vinaphone",
        "0" -> "0"
    )
    val myResMap = Map (
        "(^[0-9]){1}([0-9]){1}[0-9]{2}\\*([0-9]){1}[0-9]{2}$" -> "{1}{2}XX * {3}XX", //1024*728 ==> 10XX * 7XX
        "(^[0-9]){1}([0-9]){1}[0-9]{2}\\*([0-9]){1}([0-9]){1}[0-9]{2}$" -> "{1}{2}XX * {3}{4}XX", // 1724*1024 => 17XX * 10XX
        "(^[6-9]){1}[0-9]{2}\\*([0-9]){1}[0-9]{2}$" -> "{1}XX * {2}XX", // 720*480 => 7XX * 4XX
        "(^[0-5]){1}[0-9]{2}\\*([0-9]){1}[0-9]{2}$" -> "-lt 5XX * XXX" // 500*XXX => > 5XX * XXX
    )

    var contraMap: scala.collection.mutable.Map[String,Map[String,String]] = scala.collection.mutable.Map()
    contraMap += ("nwk" -> myNwMap)
    contraMap += ("tel" -> myTelMap)
    contraMap += ("os" -> myOsMap)
    contraMap += ("device" -> myDtyMap)
    contraMap += ("sc" -> myResMap)

    var defaultMap: scala.collection.mutable.Map[String,Map[String,String]] = scala.collection.mutable.Map()
    //defaultMap += ("nwk" -> myNwMap)
    //defaultMap += ("tel" -> myTelMap)
    defaultMap += ("os" -> myOsMap)
    //defaultMap += ("device" -> myDtyMap)

    var deviceConfig: scala.collection.mutable.Map[String,scala.collection.mutable.Map[String,Map[String,String]]] = scala.collection.mutable.Map()
    //deviceConfig += ("contra" -> contraMap)
    deviceConfig += (defaultGameCode -> defaultMap)

    def getDeviceConfig(gameCode: String) : scala.collection.mutable.Map[String,Map[String,String]] = {
        var returnValue: scala.collection.mutable.Map[String,Map[String,String]] = scala.collection.mutable.Map()
        if(deviceConfig.contains(gameCode)){
            //val t1: Option[scala.collection.mutable.Map[String,Map[String,String]]] = deviceConfig.get(gameCode)
            //returnValue = t1.get
            returnValue = deviceConfig(gameCode)
        }else{
            returnValue = deviceConfig(defaultGameCode)
        }
        returnValue
    }
}

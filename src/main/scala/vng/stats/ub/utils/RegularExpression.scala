package vng.stats.ub.utils

import scala.util.matching.Regex

object RegularExpression {
    
    // tested ok
    val IP_EXPR = new Regex("((?:[0-9]{1,3}\\.){3}[0-9]{1,3})")
    
    val LONG_EXPR = new Regex("([0-9]+)")
  
}
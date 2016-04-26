package cn.com.systex.desensitize

import scala.util.matching.Regex

/**
 * 正则匹配算法--2
 */
class RegularReplace(expr: String, replaceStr: String) extends Algorithm with Serializable{
  
  val regexArray = expr.split(";")
  
  override def run(content: String): String = {
    var result: String = content
    regexArray.foreach {
      reg =>
        result = result.replaceAll(reg, replaceStr)
    }
    result
  }
}
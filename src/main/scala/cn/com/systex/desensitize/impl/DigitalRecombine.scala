package cn.com.systex.desensitize.impl

import java.util.Random
import cn.com.systex.desensitize.Algorithm
import com.liny.util.GenerateDataUtil

/**
 * 数字重组算法--3
 */
class DigitalRecombine(startPos: Int, endPos: Int) extends Algorithm with Serializable{

  override def run(content: String): String = {
    var result: String = content
    if (startPos >= 1 && endPos > startPos && result.length() >= endPos) {
      val replaceStr = GenerateDataUtil.randomNumberStr(endPos - startPos + 1)
      result = content.substring(0, startPos - 1) + replaceStr + content.substring(endPos)
    }
    result
  }

}

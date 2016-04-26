package cn.com.systex.desensitize.impl

import cn.com.systex.desensitize.Algorithm

/**
 * 位置掩码算法 ----1
 */
class PositionMask(startPos: Int, endPos: Int, replaceStr: String) extends Algorithm with Serializable{

  override def run(content: String): String = {
    var result = content
    if (startPos >= 1 && endPos > startPos && content.length() >= endPos) {
      result = content.substring(0, startPos - 1) + replaceStr + content.substring(endPos)
    }
    result
  }

}

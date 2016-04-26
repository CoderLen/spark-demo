package cn.com.systex.desensitize.impl

import cn.com.systex.desensitize.Algorithm

/**
 * 全部替换算法--7
 */
class ReplaceAll(replaceStr: String) extends Algorithm with Serializable{

  override def run(content: String): String = {
    replaceStr
  }
  
}
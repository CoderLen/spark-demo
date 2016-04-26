package cn.com.systex.desensitize.impl

import cn.com.systex.desensitize.Algorithm
import com.liny.util.EncryptUtil

/**
 * md5加密算法--6
 */
class Md5Encryp extends Algorithm with Serializable{

  override def run(content: String): String = {
    EncryptUtil.md5(content)
  }
}

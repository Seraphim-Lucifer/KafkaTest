package com.cg

import redis.clients.jedis._

/**
 * @author SeraphimLucifar
 * @date 2019-09-05
 */
object JedisUtil {
  val cfg=new JedisPoolConfig
  cfg.setMaxTotal(10)
  cfg.setMaxIdle(10)
  val pool=new JedisPool(cfg,"cg01",6379)
  def open={
    pool.getResource
  }
  def close(jedis:Jedis): Unit ={
    jedis.close()
  }

sys.addShutdownHook(new Thread(){
  pool.close()
})
}

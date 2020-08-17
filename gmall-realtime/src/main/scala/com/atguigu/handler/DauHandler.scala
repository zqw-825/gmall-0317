package com.atguigu.handler

import java.time.LocalDate


import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

/**
 * @author zqw
 * @create 2020-08-16 21:02 
 */
object DauHandler {

  //按组同批次去重
  def filterByGroup(filterByRedisDSteam: DStream[StartUpLog]): DStream[StartUpLog] = {
    //先转换结构
    val midDateToStartLogDStream = filterByRedisDSteam.map(startLog => {
      (s"${startLog.mid}-${startLog.logDate}", startLog)
    })
    val midDateToStartLogDStreamIter = midDateToStartLogDStream.groupByKey()
    val midDateToStartLogDStreamlist = midDateToStartLogDStreamIter.mapValues(iter => {
      iter.toList.sortWith(_.ts < _.ts) take 1
    })

    midDateToStartLogDStreamlist.flatMap{
      case (k,list)=>
        list
    }

  }


  //跨批次去重 方法
  def filterByRedis(startLogStream: DStream[StartUpLog], sc: SparkContext): DStream[StartUpLog] = {
    //    //1 单条数据过滤
    //    val value1 = startLogStream.filter(startLog => {
    //      //获取连接
    //      val client = RedisUtil.getJedisClient
    //      //操作
    //      //先查询在redis中是否存在该Mid
    //      val isExist = client.sismember(s"dau:${startLog.logDate}", startLog.mid)
    //
    //      //关闭连接
    //      client.close()
    //
    //      !isExist
    //    })
    //方法二 公用连接 +
    //    val filterIter = startLogStream.mapPartitions(iter => {
    //      //获取连接
    //      val client = RedisUtil.getJedisClient
    //      //过滤
    //      iter.filter(startUpLog => {
    //        //返回过滤的boo值的反
    //        !client.sismember(s"dau:${startUpLog.logDate}", startUpLog.mid)
    //      })
    //      // 关闭连接
    //      client.close()
    //      //返回值
    //      iter
    //    })
    //    filterIter
    //方案三 广播变量 transform
    val value = startLogStream.transform(rdd => {
      //获取redis中的Set集合中的数据并广播出去,每个批次在driver执行一次
      val client = RedisUtil.getJedisClient
      val today = LocalDate.now()
      val midSet = client.smembers(s"dau:$today")
      val midSetBC = sc.broadcast(midSet)
      client.close()
      rdd.filter(StartLog => {
        !midSetBC.value.contains(StartLog.mid)
      })

    })
    value
  }


  //将两次去重之后的数据写入Redis
  def saveMidToRedis(startLogStream: DStream[StartUpLog]): Unit = {
    //遍历rdd
    startLogStream.foreachRDD(rdd => {
      //减少链接数
      rdd.foreachPartition(iter => {
        //获取连接
        val client = RedisUtil.getJedisClient
        //操作数据
        iter.foreach(startUpLog => {
          val redisKey = s"dau:${startUpLog.logDate}"
          client.sadd(redisKey, startUpLog.mid)
        })
        //归还连接
        client.close()
      })

    })
  }

}

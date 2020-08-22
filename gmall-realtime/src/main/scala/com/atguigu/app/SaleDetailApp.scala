package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean._
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization
import scala.collection.mutable.ListBuffer
import collection.JavaConverters._

/**
 * @author zqw
 * @create 2020-08-21 14:11 
 */
object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    //创建SparkConf和StreamingContext对象
    val conf = new SparkConf().setAppName("SaleApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    //Kafka工具将数据写入Kafka获取Kafka数据流
    val orderInfoKafkaDStream =
      MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_INFO, ssc)
    val orderDetailKafkaDStream =
      MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_DETAIL, ssc)

    //定义的时间格式化对象
    //val dfs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    //将JSON数据流中的数据封装为样例类 返回样例类数据流  方便处理
    val orderIdToOrderInfoDStream = orderInfoKafkaDStream.map(record => {
      //将JSON转换为样例类对象
      val orderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      //处理样例类字段
      val create_date = orderInfo.create_time
      val dateArr = create_date.split(" ")
      orderInfo.create_date = dateArr(0)
      orderInfo.create_hour = dateArr(1).split(":")(0)
      orderInfo.consignee_tel = orderInfo.consignee_tel.splitAt(4)._1 + "********"
      //返回方便处理的数据流
      (orderInfo.id, orderInfo)
    })

    //同样处理JSON
    val orderIdToDetailDStream = orderDetailKafkaDStream.map(record => {
      val orderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      (orderDetail.id, orderDetail)
    })

    //双流join   应用fullOuterJoin
    val OrderIdToInfoAndDetailDStream =
      orderIdToOrderInfoDStream.fullOuterJoin(orderIdToDetailDStream)

    val noUserSaleDetailDStream = OrderIdToInfoAndDetailDStream.mapPartitions(iter => {

      //静态导入：可将Scala对象转换为Json  放在分区里 因为不可序列化
      implicit val formats = org.json4s.DefaultFormats

      //创建redis连接
      val jedisClient = RedisUtil.getJedisClient

      //创建集合用于存放join上的数据 当前 和 前批
      val details = new ListBuffer[SaleDetail]
      //处理分区终每一条数据
      //处理逻辑-------------------------------------
      /*先判断orderId是否为null
        orderId不为 None 将orderId装换为json写入redis缓存(因为1对多)
            判断：orderDetail是否为None
            !None -> 关联上 组合为SaleOrder写入集合details
            None -> 查看redis缓存里是否有对应的OrderDetail

        orderId为None
            orderId一定不为None 查看redis缓存里是否有对应的OrderId
                                有则组合为SaleOrder写入集合details
                                没有则转化为json写到redis缓存*/
      iter.foreach {
        case (orderId, (infoOpt, detailOpt)) => {
          //定义RedisKey
          val orderInfoRedisKey = s"order_info:$orderId"
          val orderDetailRedisKey = s"order_detail:$orderId"

          //若infoOpt不为None
          if (infoOpt.isDefined) {
            //获取orderOpt中的数据 get
            val orderInfo = infoOpt.get
            //若detailOpt不为None
            if (detailOpt.isDefined) {
              val orderDetail = detailOpt.get
              //可关联 结合放入details集合
              details.append(new SaleDetail(orderInfo, orderDetail))
            }
            //写入缓存
            //将Scala样例类对象转换为Json  ！！！
            val orderInfoJson: String = Serialization.write(orderInfo)
            //写入Redis  String类型 KV
            jedisClient.set(orderInfoRedisKey, orderInfoJson)
            //定义延时时间
            jedisClient.expire(orderInfoRedisKey, 100)

            //查询前置批次 OrderDetail的数据  全体遍历
            val orderDetailJsonSet = jedisClient.smembers(orderDetailRedisKey)

              orderDetailJsonSet.asScala.foreach(orderInfoJson => {
                val orderDetail = JSON.parseObject(orderInfoJson, classOf[OrderDetail])
                details.append(new SaleDetail(orderInfo, orderDetail))
              })

            //infoOpt为None
          } else {
            //获取detailOpt的值
            val orderDetail = detailOpt.get
            //查询OrderInfo前置数据流的数据
            val orderInfoJson = jedisClient.get(orderInfoRedisKey)

            if (orderInfoJson != null) {
              //
              val orderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])

              details.append(new SaleDetail(orderInfo, orderDetail))

            } else {
              //将自身写入redis
              val orderDetailJson = Serialization.write(orderDetail)
              jedisClient.sadd(orderDetailRedisKey, orderDetailJson)
              jedisClient.expire(orderDetailRedisKey, 100)
            }
          }
        }
      }
      //---------------------------------------------
      //关闭连接
      jedisClient.close()
      details.toIterator
    })



    //启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}

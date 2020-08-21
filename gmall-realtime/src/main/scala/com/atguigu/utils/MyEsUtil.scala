package com.atguigu.utils

import java.util
import java.util.Objects

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index}
import collection.JavaConversions._


/**
 * @author zqw
 * @create 2020-08-20 21:15 
 */
object MyEsUtil {
  private val ES_HOST = "http://hadoop102"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = _

  /**
   * 获取客户端
   *
   * @return jestclient
   */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
   * 关闭客户端
   */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
   * 建立连接
   */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT)
      .multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000)
      .readTimeout(10000)
      .build)
  }

  //数据写入ES

  def insertByBulk(indexName: String, typeName: String, list: List[(String, Any)]) = {
    if (list.nonEmpty) {
      //获取链接
      val client = getClient

      //创建bulk对象
      val builder = new Bulk.Builder()
        .defaultIndex(indexName)
        .defaultType(typeName)

      //遍历list，创建index对象，并放入Bulk.Bulder对象
      list.foreach {
        case (docId, data) => {
          //给每一条数据创建index对象
          val index = new Index.Builder().id(docId).build()
          //将index对象放入Builder对象中
          builder.addAction(index)
        }
      }

      //Bulk.Bulider创建Bulk
      val bulk = builder.build()

      //执行
      client.execute(bulk)

      //释放连接
      close(client)

    }
  }


}

package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.*;

import java.net.InetSocketAddress;
import java.util.List;



/**
 * @author zqw
 * @create 2020-08-18 11:50
 */
public class CanalClient {
    public static void main(String[] args) {
        //获取Canal连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111),
                "example", "", "");


        while (true) {
            //连接
            canalConnector.connect();
            //订阅监控的表
            canalConnector.subscribe("gmall200317.*");
            //抓取数据  按条  获取message
            Message message = canalConnector.get(100);

            //处理message
            if (message.getEntries().size() <= 0) {
                System.out.println("未抓取数据");

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                //遍历message的Entry  来获取类型,表名,序列化
                for (CanalEntry.Entry entry : message.getEntries()) {
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {
                        try {
                            //获取Entry中的表名和数据
                            String tableName = entry.getHeader().getTableName();
                            ByteString storeValue = entry.getStoreValue();
                            //反序列化数据storeValue
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                            //5.获取事件类型
                            CanalEntry.EventType eventType = rowChange.getEventType();
                            //6.获取数据
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                            //7.处理数据
                            handler(tableName,eventType,rowDatasList);

                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }


                    }
                }
            }
        }


    }

    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
            //需求：只要order_info表中新增数据
            if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)){
                //遍历行
                for (CanalEntry.RowData rowData : rowDatasList) {
                    //创建一个json来存放数据
                    JSONObject jsonObject = new JSONObject();

                    //遍历改变后的rowDatasList
                    for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                        jsonObject.put(column.getName(),column.getValue());
                    }
                    System.out.println("kafka!!");

                    MyKafkaSender.send(GmallConstants.GMALL_TOPIC_ORDER_INFO,jsonObject.toString());

                }
            }
    }
}

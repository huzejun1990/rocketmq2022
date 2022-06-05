package com.dream.retry;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * @Author : huzejun
 * @Date: 2022/6/6-1:51
 */
public class SyncProducer {
    public static void main(String[] args) throws Exception {
        // 创建一个producer,参数为Producer Group名称
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        // 指定nameServer地址
        producer.setNamesrvAddr("slave1:9876");
        // 设置同步发送达失败重试的次数，默念为2次
        producer.setRetryTimesWhenSendFailed(3);
        // 设置发送超时时限为5s,默认为3s
        producer.setSendMsgTimeout(5000);
        
        //开启生产者
        producer.start();
        
        // 生产并发送100条消息
        for (int i = 0; i < 100; i++) {
            byte[] body = ("Hi," + i).getBytes();
            Message msg = new Message("someTopic", "someTag", body);
            //为消息指定key
            msg.setKeys("key-" + i);
            // 发送消息
            SendResult sendResult = producer.send(msg);
            System.out.println(sendResult);
        }
        //关闭
        producer.shutdown();
    }
}

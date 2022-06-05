package com.dream.retry;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * @Author : huzejun
 * @Date: 2022/6/6-2:30
 */
public class AsyncProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("slave1:9876");
        // 指定异步发送失败后不进行重试发送
        producer.setRetryTimesWhenSendAsyncFailed(0);
        // 指定新创建的Topic的Queue 数量为2，默认为4
        producer.setDefaultTopicQueueNums(2);

        producer.start();

        for (int i = 0; i < 100; i++) {
            byte[] body = ("Hi," + i).getBytes();
            try {
                Message msg = new Message("myTopicA", "myTag", body);
                // 异步发送，指定回调
                producer.send(msg, new SendCallback() {
                    // 当producer接收到MQ发送到的ACK后就会触发该回调 方法的执行
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.println(sendResult);
                    }

                    @Override
                    public void onException(Throwable e) {
                        e.printStackTrace();
                    }
                });
            } catch (Exception e){
                e.printStackTrace();
            }
        }   //end-for
        // sleep一会儿
        // 由于采用的是异步发送，所以若这里不sleep,则消息清空未发送就会将producer给关闭，报错
        TimeUnit.SECONDS.sleep(3);
        producer.shutdown();
    }

}

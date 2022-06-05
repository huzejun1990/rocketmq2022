package com.dream.transaction;

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.*;

/**
 * @Author : huzejun
 * @Date: 2022/6/5-2:47
 */
public class TransactionProducer {
    public static void main(String[] args) throws Exception {
        TransactionMQProducer producer = new TransactionMQProducer("tpg");
        producer.setNamesrvAddr("slave1:9876");

        /**
         *定义一个线程池
         corePoolSize – 线程池中核心线程数量
         maximumPoolSize – 线程池中最多线程数
         keepAliveTime –  这是一个时候。当线程池中线程数量大于核心线程数量时，
                            多余空闲线程的存活时长
         unit – 时间单位
         workQueue – 临时存放任务的队列，其参数是队列的长度
         threadFactory – 线程工厂
         */
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });

        // 为生产者指定一个线程池
        producer.setExecutorService(executorService);
        // 为生产者添加事务监听器
        producer.setTransactionListener(new ICBCTransactionListener());

        producer.start();

        String[] tags = {"TAGA","TAGB","TAGC"};
        for (int i = 0; i < 3; i++) {
            byte[] body = ("Hi," + i).getBytes();
            Message msg = new Message("TTopic", tags[i], body);
            // 发送事务消息
            // 第二个参数用于指定在执行本地事务时要使用的业务参数
            SendResult sendResult = producer.sendMessageInTransaction(msg, null);
            System.out.println("发送结果为： " + sendResult.getSendStatus());
        }
    }

}

package com.li.rabbitmq_demo;

import com.rabbitmq.client.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class FanoutTest {
    @Test
    public void publish() throws IOException, TimeoutException {
        // 1. 获取连接
        Connection conn = RabbitmqConfig.getConnection();

        // 2. 获取channel
        Channel channel = conn.createChannel();

        // 3. 操作
        // 定义一个exchange(交换机)
        channel.exchangeDeclare("pubsub_exchange", BuiltinExchangeType.FANOUT);

        // 定义两个queue(队列)
        channel.queueDeclare("pubsub_queue1", true, false, false, null);
        channel.queueDeclare("pubsub_queue2", true, false, false, null);

        // 将两个queue绑定到exchange上
        channel.queueBind("pubsub_queue1", "pubsub_exchange", "");
        channel.queueBind("pubsub_queue2", "pubsub_exchange", "");

        // 向交换机发送消息
        channel.basicPublish("pubsub_exchange", "", null, "Hello Pubsub!".getBytes());

        // 4. 关闭资源
        channel.close();
        conn.close();
    }

    @Test
    public void consume1() throws Exception {
        //1. 获取连接对象
        Connection connection = RabbitmqConfig.getConnection();

        //2. 创建channel
        Channel channel = connection.createChannel();

        //3. 声明队列-HelloWorld
        //参数1：queue - 指定队列的名称
        //参数2：durable - 当前队列是否需要持久化（true）
        //参数3：exclusive - 是否排外（conn.close() - 当前队列会被自动删除，当前队列只能被一个消费者消费）
        //参数4：autoDelete - 如果这个队列没有消费者在消费，队列自动删除
        //参数5：arguments - 指定当前队列的其他信息
        channel.queueDeclare("pubsub_exchange",true,false,false,null);

        //4. 开启监听Queue
        DefaultConsumer consume = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("接收到消息：" + new String(body,"UTF-8"));
            }
        };

        //参数1：queue - 指定消费哪个队列
        //参数2：autoAck - 指定是否自动ACK （true，接收到消息后，会立即告诉RabbitMQ）
        //参数3：consumer - 指定消费回调
        channel.basicConsume("pubsub_queue1",true,consume);

        System.out.println("消费者开始监听队列！");
        // System.in.read();
        System.in.read();

        //5. 释放资源
        channel.close();
        connection.close();
    }

    @Test
    public void consume2() throws Exception {
        //1. 获取连接对象
        Connection connection = RabbitmqConfig.getConnection();

        //2. 创建channel
        Channel channel = connection.createChannel();

        //3. 声明队列-HelloWorld
        //参数1：queue - 指定队列的名称
        //参数2：durable - 当前队列是否需要持久化（true）
        //参数3：exclusive - 是否排外（conn.close() - 当前队列会被自动删除，当前队列只能被一个消费者消费）
        //参数4：autoDelete - 如果这个队列没有消费者在消费，队列自动删除
        //参数5：arguments - 指定当前队列的其他信息
        channel.queueDeclare("pubsub_exchange",true,false,false,null);

        //4. 开启监听Queue
        DefaultConsumer consume = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("接收到消息：" + new String(body,"UTF-8"));
            }
        };

        //参数1：queue - 指定消费哪个队列
        //参数2：autoAck - 指定是否自动ACK （true，接收到消息后，会立即告诉RabbitMQ）
        //参数3：consumer - 指定消费回调
        channel.basicConsume("pubsub_queue2",true,consume);

        System.out.println("消费者开始监听队列！");
        // System.in.read();
        System.in.read();

        //5. 释放资源
        channel.close();
        connection.close();
    }
}

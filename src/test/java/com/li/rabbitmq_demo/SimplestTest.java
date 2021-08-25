package com.li.rabbitmq_demo;

import com.rabbitmq.client.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class SimplestTest {
    @Test
    public void publish() throws IOException, TimeoutException {
        //1. 获取Connection
        Connection connection = RabbitmqConfig.getConnection();

        //2. 创建Channel
        Channel channel = connection.createChannel();
        channel.queueDeclare("Hello-World-Queue", false, false, true, null);

        //3. 发布消息到exchange，同时指定路由的规则
        String msg = "Hello-World！";
        // 参数1：指定exchange，使用""。
        // 参数2：指定路由的规则，使用具体的队列名称。
        // 参数3：指定传递的消息所携带的properties，使用null。
        // 参数4：指定发布的具体消息，byte[]类型
        channel.basicPublish("","HelloWorld",null,msg.getBytes());
        // Ps：exchange是不会帮你将消息持久化到本地的，Queue才会帮你持久化消息。
        System.out.println("生产者发布消息成功！");
        //4. 释放资源
        channel.close();
        connection.close();
    }

    @Test
    public void consume() throws Exception {
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
        channel.queueDeclare("HelloWorld",true,false,false,null);

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
        channel.basicConsume("HelloWorld",true,consume);

        System.out.println("消费者开始监听队列！");
        // System.in.read();
        System.in.read();

        //5. 释放资源
        channel.close();
        connection.close();
    }

}

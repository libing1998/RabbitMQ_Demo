package com.li.rabbitmq_demo;

import com.rabbitmq.client.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class WorkTest {
    @Test
    public void consumer() throws IOException, TimeoutException {
        // 1. 连接
        Connection conn = RabbitmqConfig.getConnection();

        // 2. 获取channel
        Channel channel = conn.createChannel();

        // 3. 操作
        // 告诉RabbitMQ，一次只发一个消息
        // 原因是，RabbitMQ为了提升运行效率，不会一条一条地给消费者发送消息；而是多条多条地发送消息
        // 因为测试中，希望每一个消费者都有机会收到消息，所以设置QoS为1，也就是一次只发一条消息
        // 在真实场景中，不要设置
        channel.basicQos(1);

        channel.basicConsume("HelloWorld", false, new DefaultConsumer(channel) {

            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                System.out.println("第2个消费者: " + new String(body));

                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        });

        System.in.read();

        // 4. 关闭资源
        channel.close();
        conn.close();

    }
}

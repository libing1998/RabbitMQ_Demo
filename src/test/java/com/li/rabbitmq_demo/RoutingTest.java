package com.li.rabbitmq_demo;

import com.rabbitmq.client.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RoutingTest {
    @Test
    public void publish() throws IOException, TimeoutException {
        Connection connection = RabbitmqConfig.getConnection();

        Channel channel = connection.createChannel();

        channel.exchangeDeclare("routing_exchange", BuiltinExchangeType.DIRECT);

        channel.queueDeclare("routing_queue_info",true,false,false,null);
        channel.queueDeclare("routing_queue_error",true,false,false,null);

        channel.queueBind("routing_queue_info","routing_exchange","info");
        channel.queueBind("routing_queue_error","routing_exchange","error");

        channel.basicPublish("routing_exchange","info",null,"this is info msg!".getBytes());
        channel.basicPublish("routing_exchange","error",null,"this is error msg!".getBytes());


        channel.close();
        connection.close();

    }

    @Test
    public void consumeForInfo() throws Exception {
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
        channel.queueDeclare("routing_exchange",true,false,false,null);

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
        channel.basicConsume("routing_queue_info",true,consume);

        System.out.println("消费者开始监听队列！");
        // System.in.read();
        System.in.read();

        //5. 释放资源
        channel.close();
        connection.close();
    }


    @Test
    public void consumeForError() throws Exception {
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
        channel.queueDeclare("routing_exchange",true,false,false,null);

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
        channel.basicConsume("routing_queue_error",true,consume);

        System.out.println("消费者开始监听队列！");
        // System.in.read();
        System.in.read();

        //5. 释放资源
        channel.close();
        connection.close();
    }
}

package com.li.rabbitmq_demo;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class RabbitmqConfig {

    @Bean
    public static Connection getConnection(){
        // 创建Connection工厂
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.18.128");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setVirtualHost("/test");

        // 创建Connection
        Connection conn = null;
        try {
            conn = factory.newConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 返回
        return conn;
    }
}

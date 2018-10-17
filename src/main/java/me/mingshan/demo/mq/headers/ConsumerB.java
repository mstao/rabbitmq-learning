package me.mingshan.demo.mq.headers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP.BasicProperties;

/**
 * 消费者
 * @author mingshan
 *
 */
public class ConsumerB {
    private final static String EXCHANGE_NAME = "logs-headers";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.HEADERS);

        String queueName = channel.queueDeclare().getQueue();

        Map<String, Object> headers = new HashMap<String, Object>();
        headers.put("x-match", "any");//all any  
        headers.put("xiaoming", "1111");
        headers.put("bbb", "56789");
        channel.queueBind(queueName, EXCHANGE_NAME, "", headers);

        System.out.println("B Waiting for messages. To exit press CTRL+C");
        Consumer consumer = new DefaultConsumer(channel) {

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("B Recv '" + message + "'");
            }
        };

        channel.basicConsume(queueName, true, consumer);
    }
}

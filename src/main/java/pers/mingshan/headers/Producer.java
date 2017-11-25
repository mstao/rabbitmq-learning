package pers.mingshan.headers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

/**
 * 生产者
 * Exchange Types为headers
 * 
 * Headers是一个键值对，可以定义成HashMap。发送者在发送的时候定义一些键值对，接收者也可以再绑定时候传入一些键值对，
 * 两者匹配的话，则对应的队列就可以收到消息。匹配有两种方式all和any。这两种方式是在接收端必须要用键值"x-mactch"来定义
 * 。all代表定义的多个键值对都要满足，而any则代码只要满足一个就可以了。fanout，direct，topic exchange的routingKey都需要要字符串形式的，
 * 而headers exchange则没有这个要求，因为键值对的值可以是任何类型。
 * @author mingshan
 *
 */
public class Producer {
    private final static String EXCHANGE_NAME = "logs-headers";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        // 声明exchange，Exchange Types为headers
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.HEADERS);

        Map<String,Object> headers =  new HashMap<String, Object>();
        headers.put("xiaoming", "123456");
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
        builder.deliveryMode(MessageProperties.PERSISTENT_TEXT_PLAIN.getDeliveryMode());
        builder.priority(MessageProperties.PERSISTENT_TEXT_PLAIN.getPriority());
        builder.headers(headers);
        AMQP.BasicProperties theProps = builder.build();

        System.out.println("Please enter message --->");
        Scanner scanner = new Scanner(System.in);
        String message = "";

        while (scanner.hasNext()) {
            message = scanner.nextLine();
            channel.basicPublish(EXCHANGE_NAME, "", theProps, message.getBytes("UTF-8"));
        }

        channel.close();
        connection.close();
        scanner.close();
    }

}

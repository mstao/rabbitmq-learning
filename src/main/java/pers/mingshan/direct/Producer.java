package pers.mingshan.direct;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * 生产者
 * Exchange Types为direct
 * 
 * direct类型的Exchange路由规则也很简单，它会把消息路由到那些binding key与routing key完全匹配的Queue中。
 * @author mingshan
 *
 */
public class Producer {
    private final static String EXCHANGE_NAME = "logs-direct";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        // 声明exchange，Exchange Types为direct
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        System.out.println("Please enter message --->");
        String message = "";
        String routeKey = "error";
        Scanner scanner = new Scanner(System.in);

        while (scanner.hasNext()) {
            message = scanner.nextLine();
            System.out.println(" ----- " + message);
            channel.basicPublish(EXCHANGE_NAME, routeKey, null, message.getBytes("UTF-8"));
        }

        channel.close();
        connection.close();
        scanner.close();
    }

}

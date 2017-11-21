package pers.mingshan.topic;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * 发布，订阅
 * Exchange Types为topic
 * <ul>
 *   <li>routing key为一个句点号“. ”分隔的字符串（我们将被句点号“. ”分隔开的每一段独立的字符串称为一个单词），
 *     如“stock.usd.nyse”、“nyse.vmw”、“quick.orange.rabbit”</li>
 *   <li>binding key与routing key一样也是句点号“. ”分隔的字符串</li>
 *   <li>binding key中可以存在两种特殊字符“*”与“#”，用于做模糊匹配，其中“*”用于匹配一个单词，“#”用于匹配多个单词（可以是零个）</li>
 * </ul>
 * @author mingshan
 *
 */
public class Publisher {
    private final static String EXCHANGE_NAME = "logs-topic";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        // 声明exchange，Exchange Types为headers
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        System.out.println("Please enter message --->");
        String message = "";
        String routeKey = "quick.orange.rabbit";
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

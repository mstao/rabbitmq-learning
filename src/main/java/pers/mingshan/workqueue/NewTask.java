package pers.mingshan.workqueue;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class NewTask {
    private static final String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();
        // 消息持久化， 将durable设置为true
        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

        System.out.println("Please enter message --->");
        String message = ""; // hello.....
        Scanner scanner = new Scanner(System.in);

        while (scanner.hasNext()) {
            message = scanner.nextLine();
            System.out.println(" ----- " + message);
            channel.basicPublish("", 
                    TASK_QUEUE_NAME, 
                    MessageProperties.PERSISTENT_TEXT_PLAIN,
                    message.getBytes("UTF-8"));
        }

        channel.close();
        conn.close();
        scanner.close();
    }
}

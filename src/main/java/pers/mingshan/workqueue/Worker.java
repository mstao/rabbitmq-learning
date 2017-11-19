
package pers.mingshan.workqueue;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Worker {
    private static final String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();
        // 消息持久化， 将durable设置为true
        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        // 设置rabbitmq 同一时间发送一个消息给消费者
        channel.basicQos(1);

        final Consumer consumer = new DefaultConsumer(channel) {

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + message + "'");
                try{
                    work(message);
                } finally {
                    System.out.println(" [x] Done");
                    // 发送回执
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };

        // 将autoAck置为false
        channel.basicConsume(TASK_QUEUE_NAME , false, consumer);
    }

    protected static void work(String message) {
        for (char s : message.toCharArray()) {
            if (s == '.') {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}

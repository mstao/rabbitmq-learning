package me.mingshan.demo.mq.rpc;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * 步骤：
 * <ul>
 *    <li>客户端发送请求（消息）时，在消息的属性（MessageProperties，在AMQP协议中定义了14中properties，
 *        这些属性会随着消息一起发送）中设置两个值replyTo（一个Queue名称，
 *        用于告诉服务器处理完成后将通知我的消息发送到这个Queue中）和correlationId（此次请求的标识号，
 *        服务器处理完成后需要将此属性返还，客户端将根据这个id了解哪条请求被成功执行了或执行失败）
 *    </li>
 *    <li>服务器端收到消息并处理</li>
 *    <li>服务器端处理完消息后，将生成一条应答消息到replyTo指定的Queue，同时带上correlationId属性</li>
 *    <li>客户端之前已订阅replyTo指定的Queue，从中收到服务器的应答消息后，
 *        根据其中的correlationId属性分析哪条请求被执行了，根据执行结果进行后续业务处理
 *    </li>
 * </ul>
 * @author mingshan
 *
 */
public class RPCServer {
    private static final String RPC_QUEUE_NAME = "rpc_queue";

    private static int fib(int n) {
      if (n ==0) return 0;
      if (n == 1) return 1;
      return fib(n-1) + fib(n-2);
    }

    public static void main(String[] args) {
      ConnectionFactory factory = new ConnectionFactory();
      factory.setHost("localhost");

      Connection connection = null;
      try {
        connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);

        channel.basicQos(1);

        System.out.println(" [x] Awaiting RPC requests");
        // 创建消费者
        Consumer consumer = new DefaultConsumer(channel) {
          @Override
          public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                  throws IOException {
            // 获取请求中correlationId属性，并将其设置到响应消息的correlationId中
            AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(properties.getCorrelationId())
                    .build();

            String response = "";

            try {
              // 获取请求的内容
              String message = new String(body,"UTF-8");
              int n = Integer.parseInt(message);

              System.out.println(" [.] fib(" + message + ")");
              response += fib(n);
            } catch (RuntimeException e){
              System.out.println(" [.] " + e.toString());
            } finally {
              // 发送消息 到指定的queue : properties.getReplyTo()
              channel.basicPublish( "", properties.getReplyTo(), replyProps, response.getBytes("UTF-8"));
              // 发送回执
              channel.basicAck(envelope.getDeliveryTag(), false);
              // RabbitMq consumer worker thread notifies the RPC server owner thread 
              synchronized(this) {
                  this.notify();
              }
            }
          }
        };

        channel.basicConsume(RPC_QUEUE_NAME, false, consumer);
        // Wait and be prepared to consume the message from RPC client.
        while (true) {
          synchronized(consumer) {
              try {
                  consumer.wait();
              } catch (InterruptedException e) {
                  e.printStackTrace();
              }
          }
        }
      } catch (IOException | TimeoutException e) {
        e.printStackTrace();
      } finally {
        if (connection != null)
          try {
            connection.close();
          } catch (IOException ignore) {}
      }
    }
}

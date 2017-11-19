package pers.mingshan.rpc;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
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
public class RPCClient {
    private Connection connection;
    private Channel channel;
    private String requestQueueName = "rpc_queue";
    private String replyQueueName;

    public RPCClient() throws IOException, TimeoutException {
      ConnectionFactory factory = new ConnectionFactory();
      factory.setHost("localhost");

      connection = factory.newConnection();
      channel = connection.createChannel();
      // 接收服务端返回消息的队列
      replyQueueName = channel.queueDeclare().getQueue();
    }

    public String call(String message) throws IOException, InterruptedException {
      String corrId = UUID.randomUUID().toString();
      // 将replayTo 和 correlationId放到消息属性中发送服务端
      AMQP.BasicProperties props = new AMQP.BasicProperties
              .Builder()
              .correlationId(corrId)
              .replyTo(replyQueueName)
              .build();

      channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));

      // 定义一个阻塞队列，将服务端返回的消息放到阻塞队列中
      final BlockingQueue<String> response = new ArrayBlockingQueue<String>(1);

      // 消费服务端返回过来的消息
      channel.basicConsume(replyQueueName, true, new DefaultConsumer(channel) {
        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
          if (properties.getCorrelationId().equals(corrId)) {
            response.offer(new String(body, "UTF-8"));
          }
        }
      });

      return response.take();
    }

    public void close() throws IOException {
      connection.close();
    }

    public static void main(String[] argv) {
      RPCClient fibonacciRpc = null;
      String response = null;
      try {
        fibonacciRpc = new RPCClient();

        System.out.println(" [x] Requesting fib(30)");
        response = fibonacciRpc.call("30");
        System.out.println(" [.] Got '" + response + "'");
      } catch  (IOException | TimeoutException | InterruptedException e) {
        e.printStackTrace();
      } finally {
        if (fibonacciRpc!= null) {
          try {
            fibonacciRpc.close();
          } catch (IOException _ignore) {}
        }
      }
    }
}

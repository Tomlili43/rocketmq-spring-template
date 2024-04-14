package com.example.service;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.java.message.MessageBuilderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelayMQ {
  private static final Logger logger = LoggerFactory.getLogger(ProducerExample.class);

  public static void main(String[] args) throws ClientException {
    // 接入点地址，需要设置成Proxy的地址和端口列表，一般是xxx:8081;xxx:8081。
    String endpoint = "localhost:8081";
    // 消息发送的目标Topic名称，需要提前创建。
    String topic = "DelayTopic";
    ClientServiceProvider provider = ClientServiceProvider.loadService();
    ClientConfigurationBuilder builder = ClientConfiguration.newBuilder().setEndpoints(endpoint);
    ClientConfiguration configuration = builder.build();
    //定时/延时消息发送
    MessageBuilderImpl messageBuilder = new MessageBuilderImpl();
    //以下示例表示：延迟时间为10分钟之后的Unix时间戳。
    Long deliverTimeStamp = System.currentTimeMillis() + 10L * 60 * 1000;  
    
    // 初始化Producer时需要设置通信配置以及预绑定的Topic。
    Producer producer = provider.newProducerBuilder()
        .setTopics(topic)
        .setClientConfiguration(configuration)
        .build();
    Message message = messageBuilder.setTopic("DelayTopic")
    //设置消息索引键，可根据关键字精确查找某条消息。
    .setKeys("DelayTopic-Key")
    //设置消息Tag，用于消费端根据指定Tag过滤消息。
    .setTag("DelayTopic-Tag")
    .setDeliveryTimestamp(deliverTimeStamp)
    //消息体
    .setBody("DelayTopic-Body".getBytes())
    .build();
    try {
      //发送消息，需要关注发送结果，并捕获失败等异常。
      SendReceipt sendReceipt = producer.send(message);
      System.out.println(sendReceipt.getMessageId());
    } catch (ClientException e) {
      e.printStackTrace();
    }

    // producer.close();
  }
}

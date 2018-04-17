package cn.jxust.bigdata.kafka.simple;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Logger;

/**
 * 这是一个简单的Kafka producer代码
 * 包含两个功能:
 * 1、数据发送
 * 2、数据按照自定义的partition策略进行发送
 * 还有另一套jar包  kafka-clients 代码书写有点不一样，这里使用org.apache.kafka下的kafka_2.8.2下的0.8.1jar
 */
public class KafkaProducerSimple {
    public static void main(String[] args) {
//    	Logger logger = Logger.getLogger(KafkaProducerSimple.class);
    	
        /**
         * 1、指定当前kafka producer生产的数据的目的地
         *  创建topic可以输入以下命令，在kafka集群的任一节点进行创建。
         *  bin/kafka-topics.sh --create --zookeeper master:2181 --replication-factor 1 --partitions 4 --topic orderMq
         *  
         *  kafka-console-consumer.sh --zookeeper master:2181 --from-beginning --topic orderMq 消费消息
         */
        String TOPIC = "orderMq";
        /**
         * 2、读取配置文件
         */
        Properties props = new Properties();
        /*
         * key.serializer.class默认为serializer.class  会将消息的k，v转成bytes数组，会报错无法转换，所以最好指定为StringEncoder！！
		 */
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        /*
		 * kafka broker对应的主机，格式为host1:port1,host2:port2
		 */
        props.put("metadata.broker.list", "master:9092,slave1:9092,slave2:9092");
        /*
         * request.required.acks,设置发送数据是否需要服务端的反馈,有三个值0,1,-1
		 * 0，意味着producer永远不会等待一个来自broker的ack，这就是0.7版本的行为。
		 * 这个选项提供了最低的延迟，但是持久化的保证是最弱的，当server挂掉的时候会丢失一些数据。
		 * 1，意味着在leader replica已经接收到数据后，producer会得到一个ack。
		 * 这个选项提供了更好的持久性，因为在server确认请求成功处理后，client才会返回。
		 * 如果刚写到leader上，还没来得及复制leader就挂了，那么消息才可能会丢失。
		 * -1，意味着在所有的ISR都接收到数据后，producer才得到一个ack。
		 * 这个选项提供了最好的持久性，只要还有一个replica存活，那么数据就不会丢失
		 */
        props.put("request.required.acks", "1");
        /*
		 * 可选配置，如果不配置，则使用默认的partitioner partitioner.class
		 * 默认值：kafka.producer.DefaultPartitioner
		 * 用来把消息分到各个partition中，默认行为是对key进行hash。
		 */
        props.put("partitioner.class", "cn.jxust.bigdata.kafka.simple.MyLogPartitioner");
        
        /**
         * 3、通过配置文件，创建生产者
         */
        Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(props));
        /**
         * 4、通过for循环生产数据
         */
        for (int messageNo = 1; messageNo < 5000; messageNo++) {
            /**
             * 5、调用producer的send方法发送数据
             * 注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发
             * 第一个泛型指定用于分区的key的类型，第二个泛型指message的类型 这里的key为messageNo
             */
            producer.send(new KeyedMessage<String, String>(TOPIC, messageNo + "", "appid" + UUID.randomUUID() + "jxust"));
//            producer.send(new KeyedMessage<String, String>(TOPIC, String.valueOf(messageNo), "appid" + UUID.randomUUID() + "jxust"));
            
            
            /*Kafka会根据传进来的key计算其分区ID。但是这个Key可以不传，根据Kafka的官方文档描述：如果key为null，那么Producer将会把这条消息发送给随机的一个Partition。
            	producer.send(new KeyedMessage<String, String>(TOPIC, "mesage1"));//key为null
            */
        }
    }
}

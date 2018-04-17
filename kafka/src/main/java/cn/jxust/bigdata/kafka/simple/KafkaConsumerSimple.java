package cn.jxust.bigdata.kafka.simple;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
 * 还有另一套jar包  kafka-clients 代码书写有点不一样，这里使用org.apache.kafka下的kafka_2.8.2下的0.8.1jar
 * http://blog.csdn.net/dreamsigel/article/details/53836427
 * 消费者
 * storm中的spout是kafka的消费者，但是让每个spout的每个线程消费一个topic的分区有点难，
 * 所以有storm-kafka-0.9.6.jar 专门整合kafka和spout的
 */
public class KafkaConsumerSimple implements Runnable {
    public String title;
    public KafkaStream<byte[], byte[]> stream;
    
    public KafkaConsumerSimple(String title, KafkaStream<byte[], byte[]> stream) {
        this.title = title;
        this.stream = stream;
    }
    
    //消费组中每个线程对应一个流，即一个分区，去消费各自的数据
    public void run() {
        System.out.println("开始运行 " + title);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        /**
         * 不停地从stream读取新到来的消息，在等待新的消息时，hasNext()会阻塞
         * 如果调用 `ConsumerConnector#shutdown`，那么`hasNext`会返回false
         * */
        while (it.hasNext()) {
            MessageAndMetadata<byte[], byte[]> data = it.next();//消息和元数据
            //得到各种信息
            String topic = data.topic();
            int partition = data.partition();//获取分区
            long offset = data.offset();
            String msg = new String(data.message());
            System.out.println(String.format(
                    "Consumer: [%s],  Topic: [%s],  PartitionId: [%d], Offset: [%d], msg: [%s]",
                    title, topic, partition, offset, msg));
        }
        
        System.out.println(String.format("Consumer: [%s] exiting ...", title));
    }

    public static void main(String[] args) throws Exception{
    	
    	//配置信息
        Properties props = new Properties();
        props.put("group.id", "bigdata");//group.id是标识消费者的ID。每一个group.id消费后，kafka会记录该id消费的offset到zookeeper的/consumers下。
        props.put("zookeeper.connect", "master:2181,slave1:2181,slave2:2181");//zk地址
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key、value反序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //消费者标识（区分消费者offset）信息
        props.put("auto.offset.reset", "largest");//如果第一次读，默认值为largest从最近的开始读，如果读过，从zk的offset值开始读。设置为smallest表示从头读，但是之前读过还要设置一个新的groupid才可以，只设置一个新组也没用
//        kafka-consumer-groups.sh --bootstrap-server 172.17.6.10:9092 --describe --group bigdata 查看组的offset信息
        props.put("auto.commit.interval.ms", "1000");
        props.put("partition.assignment.strategy", "roundrobin");
        
        ConsumerConfig config = new ConsumerConfig(props);
        
        String topic1 = "orderMq";//要消费的主题
        String topic2 = "paymentMq";
        
        //只要ConsumerConnector还在的话，consumer会一直等待新消息，不会自己退出
        ConsumerConnector consumerConn = Consumer.createJavaConsumerConnector(config);//创建连接
        
        //定义一个map
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic1, 4);//这里的4表示 我要消费topic1，你kafka给我返回四个流 
//        topicCountMap.put(topic2, 3);//可以消费多个topic
        
        //Map<String, List<KafkaStream<byte[], byte[]>> 中String是topic， List<KafkaStream<byte[], byte[]>是对应的流
        Map<String, List<KafkaStream<byte[], byte[]>>> topicStreamsMap = consumerConn.createMessageStreams(topicCountMap);
        
        //取出 `orderMq` 对应的 streams
        List<KafkaStream<byte[], byte[]>> streams = topicStreamsMap.get(topic1);
        
        //创建一个容量为4的线程池
        ExecutorService executor = Executors.newFixedThreadPool(4);
        
        //我拿到4个topic1的流，一个流对应一个分区，我用4个线程去消费刚刚好
        /*
         * 最好是主题有几个分区我就获取几个流，然后每个流用一个线程去消费，也就是一个组中的消费者线程对应一个分区，如果消费者线程多于分区数，那么有的线程就会空闲，少于就会有的线程干多份工作
         */
        for (int i = 0; i < streams.size(); i++)
            executor.execute(new KafkaConsumerSimple("消费者" + (i + 1), streams.get(i)));
    }
}
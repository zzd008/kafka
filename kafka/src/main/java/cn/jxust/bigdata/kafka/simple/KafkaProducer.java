package cn.jxust.bigdata.kafka.simple;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;//注意别导错包
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import scala.collection.Seq;

/*
 * kafka生产者，这里使用org.apache.kafka下的kafka_2.8.2下的0.8.1jar
 * 还有另一套jar包  kafka-clients 代码书写有点不一样
 */
public class KafkaProducer {
	private static final String TOPIC="orderMq";//指定topic
	
	public static void main(String[] args) {
		Properties p=new Properties();//创建配置文件
		// 指定序列化类  默认为serializer.class 会将消息的k，v转成bytes数组，会报错无法转换，所以最好指定为StringEncoder
		p.put("serializer.class", "kafka.serializer.StringEncoder");
		//指定broker所在的机器
		p.put("metadata.broker.list", "master:9092,slave1:9092,slave2:9092");
		//默认值：kafka.producer.DefaultPartitioner
		//用来把消息分到各个partition中，默认行为是对key进行hash。
//		p.put("partitioner.class", "xxxxx");
		
		
		//构建生产者对象
		ProducerConfig producerConfig = new ProducerConfig(p);
		//泛型：消息中的key、value的类型  生产者在发送消息时可以为消息制定一个key用于分区
		Producer<String, String> producer = new Producer<String, String>(producerConfig);
		for(int i=0;i<5;i++){
			String message="message_"+i;//要发送的message
			// 第一个泛型指定用于分区的key的类型，第二个泛型指message的类型
			//这里参数中TOPIC为主题，message为消息value，key没写为null，这样不会根据key去hash分区而是随机分区
			 //Kafka会根据传进来的key计算其分区ID。但是这个Key可以不传，根据Kafka的官方文档描述：如果key为null，那么Producer将会把这条消息发送给随机的一个Partition。
			KeyedMessage<String,String> keyedMessage = new KeyedMessage<String, String>(TOPIC, message); //如按照ip分组就把ip当成key
			producer.send(keyedMessage);//一条一条发送消息
			
			//使用kafka-clients这个jar包，ProducerRecord用法和KeyedMessage类似，生产者在发送消息的过程中，会自己默认批量提交。所以，如果单条指令的发送请求，记得发送完后flush才能生效。
//			producer.send(new ProducerRecord<String, String>(TOPIC, Integer.toString(i), message));
//			producer.send(new ProducerRecord<String, String>(TOPIC, message));
			
		}
		System.out.println("生产成功！");
		
		//发送多条消息
		/*List<KeyedMessage<String, String>> list=new ArrayList<KeyedMessage<String, String>>();
		for(int i=0;i<5;i++){
			list.add(new KeyedMessage<String, String>(TOPIC, "message_multi"+i));
		}
		producer.send(list);*/
		
		
	}
}

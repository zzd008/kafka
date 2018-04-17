package cn.jxust.bigdata.kafka.simple;

import kafka.producer.KeyedMessage;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import java.util.UUID;

import org.apache.log4j.Logger;

/*
 * 自定义分区类
 */
public class MyLogPartitioner implements Partitioner {
    private static Logger logger = Logger.getLogger(MyLogPartitioner.class);

    //不写这个构造方法，会报错Exception in thread "main" java.lang.NoSuchMethodException: cn.jxust.bigdata.kafka.simple.MyLogPartitioner.<init>(kafka.utils.VerifiableProperties
    public MyLogPartitioner(VerifiableProperties props) {
    	
    }
    

    //obj为传进来的key，也就是new KeyedMessage<String, String>(TOPIC, key,message)中指定的用于分区的key
    public int partition(Object obj, int numPartitions) {
        return Integer.parseInt(obj.toString())%numPartitions;
//        return obj.toString().hashCode()%numPartitions;//根据key的hash取模
//        return 1;//无论传进来什么，都放在一个分区中 这里就放在了orderMq-1中
    }

}

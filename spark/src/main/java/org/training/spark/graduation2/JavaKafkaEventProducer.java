package org.training.spark.graduation2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.spark_project.guava.util.concurrent.RateLimiter;
import org.training.spark.util.KafkaRedisConfig;

import java.io.*;
import java.util.Properties;

/**
 * Created by qinghua.liu on 3/29/18.
 * 1、编写Kafka produer 程序，将user_pay 数据依次写入kafka 中的 user_pay 主题中，每条数据写入间隔为10 毫秒，其中user_id 为key，
 shop_id+”,”+time_stamp 为value
 */
public class JavaKafkaEventProducer {

    // config
    public static Properties getConfig()
    {
        Properties props = new Properties();
        //props.put("bootstrap.servers", "localhost:9092");
        props.put("bootstrap.servers", KafkaRedisConfig.KAFKA_ADDR);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public static void main(String[] args) throws Exception {
        String dataPath = "D:\\bigdata\\source\\auratrainingproject\\spark\\data\\IJCAI17_dataset";

        String topic = KafkaRedisConfig.KAFKA_USER_PAY_TOPIC;
        Properties props = getConfig();
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        //准备文件路径
        if(args.length > 0) {
            dataPath = args[0];
        }
        String fileName =  JavaSQLAliPayAnalyzer.getOSPath(dataPath+"/user_pay.txt");

        //使用RateLimiter做流量控制
        int maxRatePerSecond = 10;
        RateLimiter limiter = RateLimiter.create(maxRatePerSecond);;

        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            //一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null){
                //显示行号
                //System.out.println("line[" + line + "]=" + tempString);
                //准备数据
                String[] row = tempString.split(",");
                if(row.length>=3) {
                    limiter.acquire();//每10ms产生1个消息
                    String key = "" + row[0];//user_id
                    String value = "" + row[1] + "," + row[2];//shop_id+”,”+time_stamp
                    // 推送数据
                    producer.send(new ProducerRecord(topic, key, value));
                    System.out.println("Message[" + line + "] sent: " + key + "=>" + value);
                    producer.send(new ProducerRecord(topic, key, value));
                    line++;
//                    Thread.sleep(10);
                }
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null){
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }

    }
}
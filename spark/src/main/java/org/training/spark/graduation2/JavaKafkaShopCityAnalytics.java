package org.training.spark.graduation2;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.training.spark.util.JavaRedisClient;
import org.training.spark.util.KafkaRedisConfig;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by qinghua.liu on 3/29/18.
 * 2、编写spark streaming 程序，依次读取kafka 中user_pay 主题数据，并统计：
 a）每个商家实时交易次数，并存入redis，其中key为”jiaoyi+<shop_id>”, value 为累计的次数
 b）每个城市发生的交易次数，并存储redis，其中key 为“交易+<城市名称>”,value 为累计的次数
 */
public class JavaKafkaShopCityAnalytics {

    public static Map<String, String> shopCityMap = null;
    public static Map<String, String> getShopCityMap(String dataPath){
        Map<String, String> retMap = new HashMap<String, String>();
        String fileName =  JavaSQLAliPayAnalyzer.getOSPath(dataPath+"/shop_info.txt");

        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            //一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null){
                String[] row = tempString.split(",");
                if(row.length>=2) {
                    String key = "" + row[0];//shop_id
                    String value = "" + row[1];//city
                    retMap.put(key, value);
                    line++;
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

        return retMap;
    }

    public static String getCityOfShop(String shopId, String dataPath){
        if(shopCityMap==null || shopCityMap.isEmpty()){
            shopCityMap = getShopCityMap(dataPath);
        }

        return shopCityMap.get(shopId);
    }

    public static String dataPath = "D:\\bigdata\\source\\auratrainingproject\\spark\\data\\IJCAI17_dataset";
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("JavaKafkaShopCityAnalytics");
        if (args.length == 0) {
            conf.setMaster("local[1]");
        }else {
            dataPath = args[0];
        }

        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

        //从MySQL或文本中读取数据库，作为广播变量共享使用
        Map<String, String> shopCityMap = getShopCityMap(dataPath);
        JavaSparkContext jsc  = ssc.sparkContext();
        Broadcast<Map<String, String>> broadcastCountryMap = jsc.broadcast(shopCityMap);

        // Kafka configurations
        String[] topics = KafkaRedisConfig.KAFKA_USER_PAY_TOPIC.split("\\,");
        System.out.println("Topics: " + Arrays.toString(topics));

        String brokers = KafkaRedisConfig.KAFKA_ADDR;
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("serializer.class", "kafka.serializer.StringEncoder");

        final String clickHashKey = "app::shop::paycount";

        // Create a direct stream
        JavaPairInputDStream<String, String> kafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, String.class,
                StringDecoder.class, StringDecoder.class,
                kafkaParams,
                new HashSet<String>(Arrays.asList(topics)));

        JavaDStream events = kafkaStream.map(new Function<Tuple2<String, String>, String[]>() {
            @Override
            public String[] call(Tuple2<String, String> line) throws Exception {
                System.out.println("line:" + line._1()+"=>"+ line._2().split(",")[0]);
                String[] data = new String[]{line._1(),line._2().split(",")[0]};
                return data;
            }
        });

        // Compute user click times
        JavaPairDStream<String, Long> shopClicks = events.mapToPair(
                new PairFunction<String[], String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(String[] x) {
                        return new Tuple2<>(x[1], new Long(1));
                    }
                }).reduceByKey(new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long i1, Long i2) {
                        return i1 + i2;
                    }
                });
        shopClicks.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            @Override
            public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> partitionOfRecords) throws Exception {
                        Jedis jedis = JavaRedisClient.get().getResource();
                        while(partitionOfRecords.hasNext()) {
                            try {
                                Tuple2<String, Long> pair = partitionOfRecords.next();
                                String shopidKey = "jiaoyi"+pair._1 ();

                                //读取广播变量Map，根据shopd获取cityName
                                String cityName = broadcastCountryMap.getValue().get(pair._1 ());
                                String cityKey = "交易"+cityName;
                                //String cityKey = "交易"+getCityOfShop(pair._1 (),dataPath);

                                //交易量
                                long clickCount = pair._2();

                                //将店铺交易增量写入Redis
                                jedis.incrBy(shopidKey, clickCount);
                                System.out.println("Update shop " + shopidKey + " inc " + clickCount);

                                //将城市交易增量写入Redis
                                jedis.incrBy(cityKey, clickCount);
                                System.out.println("Update city " + cityKey + " inc " + clickCount);

                            } catch(Exception e) {
                                System.out.println("error:" + e);
                            }
                        }
                        jedis.close();
                    }
                });
            }
        });

        ssc.start();
        ssc.awaitTermination();
    }
}

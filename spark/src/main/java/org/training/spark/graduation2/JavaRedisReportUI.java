package org.training.spark.graduation2;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
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
 * 3、编写UI程序，将存入Redis中的数据读取并展示
 a）读取每个商家实时交易次数，redis，其中key为”jiaoyi+<shop_id>”, value 为累计的次数
 b）读取每个城市发生的交易次数，redis，其中key 为“交易+<城市名称>”,value 为累计的次数
 */
public class JavaRedisReportUI {
    public static List<String> shopIdList = new ArrayList<String>();
    public static List<String> cityNameList = new ArrayList<String>();
    public static void getShopCityList(String dataPath){
        shopIdList.clear();
        cityNameList.clear();
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
                    String shopId = "" + row[0];//shop_id
                    String cityName = "" + row[1];//city
                    if(!shopIdList.contains(shopId)){
                        shopIdList.add(shopId);
                    }
                    if(!cityNameList.contains(cityName)){
                        cityNameList.add(cityName);
                    }
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
    }

    public static void checkData(String dataPath){
        if(shopIdList.isEmpty() || cityNameList.isEmpty()){
            getShopCityList(dataPath);
        }
    }

    public static String dataPath = "D:\\bigdata\\source\\auratrainingproject\\spark\\data\\IJCAI17_dataset";
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            ;
        }else {
            dataPath = args[0];
        }
        checkData(dataPath);
        while (true) {
            Jedis jedis = JavaRedisClient.get().getResource();

            System.out.println("\r\n//////////////////////  商家交易量统计信息start  //////////////////////"+new Date());
            int idx = 0;
            for (String shopId : shopIdList) {
                String val = jedis.get("jiaoyi" + shopId);
                if (val != null && val.length() > 0) {
                    System.out.println("商家[" + (++idx) + "]" + shopId + "的交易笔数：" + val);
                }
            }

            System.out.println("//////////////////////  城市交易量统计信息start  //////////////////////"+new Date());
            idx = 0;
            for (String cityName : cityNameList) {
                String val = jedis.get("交易" + cityName);
                if (val != null && val.length() > 0) {
                    System.out.println("城市[" + (++idx) + "]" + cityName + "的交易笔数：" + val);
                }
            }
            jedis.close();

            Thread.sleep(5000);
        }
    }
}

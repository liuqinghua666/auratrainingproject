package org.training.spark.graduation2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by qinghua.liu on 3/28/18.
 * （4）任务4
 利用Spark RDD 或Spark DataFrame 分析产生以下结果：
 • 平均日交易额最大的前10 个商家，并输出他们各自的交易额
 • 分别输出北京、上海和广州三个城市最受欢迎的10 家火锅商店编号
 o 最受欢迎是指以下得分最高：0.7 ✖(平均评分/5) + 0.3 ✖ (平均消费金额/最高消费金额)
 */
public class JavaSQLAliPayAnalyzer {
    //判断当前系统是否windows
    public static boolean isWindows() {
        return System.getProperties().getProperty("os.name").toUpperCase().indexOf("WINDOWS") != -1;
    }

    //根据操作系统替换目录
    public static String getOSPath(String filePath){
        if(isWindows()){
            return filePath.replaceAll("/", "\\\\");
        }else {
            return filePath.replaceAll("\\\\", "/");
        }
    }

    public static Dataset<Row> getShopInfoDS(SparkSession spark, String dataPath) {
        final String textInput = getOSPath(dataPath+"/shop_info.txt");
        JavaRDD<String> userRDD = spark.sparkContext()
                .textFile(textInput, 1)
                .toJavaRDD();//.filter(s -> s.split(",").length>=10);

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
//        for (String fieldName : "shop_id,city_name,location_id,per_pay,score,comment_cnt,shop_level,cate_1_name,cate_2_name,cate_3_name".split(",")) {
//            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
//            fields.add(field);
//        }
        fields.add(DataTypes.createStructField("shop_id", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("location_id", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("per_pay", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("comment_cnt", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("shop_level", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("cate_1_name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("cate_2_name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("cate_3_name", DataTypes.StringType, true));

        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> userRowRDD = userRDD.map(new Function<String, Row>() {
            public Row call(String s) {
                String[] attr = s.split(",");
                return RowFactory.create(attr.length<=0?null:attr[0],
                        attr.length<=1?null:attr[1],
                        attr.length<=2?null:attr[2],
                        getInteger(attr.length<=3?null:attr[3]),
                        getInteger(attr.length<=4?null:attr[4]),
                        getInteger(attr.length<=5?null:attr[5]),
                        getInteger(attr.length<=6?null:attr[6]),
                        attr.length<=7?null:attr[7],
                        attr.length<=8?null:attr[8],
                        attr.length<=9?null:attr[9]
                );
            }
        });
        return spark.createDataFrame(userRowRDD, schema);
    }

    public static Dataset<Row> getUserPayDS(SparkSession spark, String dataPath) {
        final String textInput = getOSPath(dataPath+"/user_pay.txt");
        JavaRDD<String> userRDD = spark.sparkContext()
                .textFile(textInput, 1)
                .toJavaRDD();//.filter(s -> s.split(",").length>=3);

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : "user_id,shop_id,time_stamp".split(",")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }

        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> userRowRDD = userRDD.map(new Function<String, Row>() {
            public Row call(String s) {
                String[] attr = s.split(",");
                return RowFactory.create(attr.length<=0?null:attr[0],
                        attr.length<=1?null:attr[1],
                        attr.length<=2?null:attr[2]);
            }
        });
        return spark.createDataFrame(userRowRDD, schema);
    }

    private static Integer getInteger(String s){
        try {
            return (s == null || s.length()==0 )? null : new Integer(s);
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        String dataPath = "D:\\bigdata\\source\\auratrainingproject\\spark\\data\\IJCAI17_dataset";
        //String dataPath = "file:///D:/bigdata/source/auratrainingproject/data/IJCAI17_dataset";

        SparkConf conf = new SparkConf().setAppName("JavaSQLAliPayAnalyzer");
        if(args.length > 0) {
            dataPath = args[0];
        } else {
            conf.setMaster("local[*]");
        }
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        Dataset<Row>  shopDs = getShopInfoDS(spark, dataPath);
        shopDs.registerTempTable("shop_info");

        Dataset<Row>  payDs = getUserPayDS(spark, dataPath);
        payDs.registerTempTable("user_pay");

        //shop_id,city_name,location_id,per_pay,score,comment_cnt,shop_level,cate_1_name,cate_2_name,cate_3_name
        System.out.println("^^^^^^^^-------平均日交易额最大的前10 个商家，并输出他们各自的交易额-----");

//        //每个店铺的信息
//        System.out.println("^^^^^^^^-------11每个店铺的基本信息-----");
//        Dataset<Row>  dsShopOfBase = spark.sql("select * from shop_info order by shop_id limit 10");
//        dsShopOfBase.show();
//
//        //每个店铺的总交易笔数、交易日数:
//        System.out.println("^^^^^^^^-------12店铺的总交易笔数、交易日数-----");
//        Dataset<Row>  dsShopOfCountPay = spark.sql("select shop_id, count(user_id) as countPay, count(distinct substr(time_stamp, 1, 10)) as countDay from user_pay group by shop_id order by countPay desc limit 10");
//        dsShopOfCountPay.show();

        //计算日均交易额最大的10个商家：
        //select (CASE WHEN countDay>0 THEN (countPay*per_pay/countDay) ELSE 0.0 END) as avgAmountPerDay, shop_pay.*, shop_info.* from shop_info join  (select shop_id, count(user_id) as countPay, count(distinct substr(time_stamp, 1, 10)) as countDay from user_pay group by shop_id) as shop_pay on shop_info.shop_id=shop_pay.shop_id  order by avgAmountPerDay desc limit 10;
        System.out.println("^^^^^^^^-------13平均日交易额最大的前10 个商家-----");
        Dataset<Row>  dsShopOfPayTop10 = spark.sql("select (CASE WHEN countDay>0 THEN (countPay*per_pay/countDay) ELSE 0.0 END) as avgAmountPerDay, shop_pay.*, shop_info.* from shop_info join  (select shop_id, count(user_id) as countPay, count(distinct substr(time_stamp, 1, 10)) as countDay from user_pay group by shop_id) as shop_pay on shop_info.shop_id=shop_pay.shop_id  order by avgAmountPerDay desc limit 10"
        );
        dsShopOfPayTop10.show();

        System.out.println("^^^^^^^^-------分别输出北京、上海和广州三个城市最受欢迎的10 家火锅商店编号-----");
        System.out.println("^^^^^^^^最受欢迎是指以下得分最高：0.7 ✖(平均评分/5) + 0.3 ✖ (平均消费金额)");

        Dataset<Row>  dsShopOfWelcomeTop10 = spark.sql("select (0.7*score/5+0.3*per_pay) as welcomeScore, * from shop_info where city_name='北京' order by (0.7*score/5+0.3*per_pay) desc limit 10");
        System.out.println("^^^^^^^^-------2北京最受欢迎的10 家火锅商店编号-----");
        dsShopOfWelcomeTop10.show((int) dsShopOfWelcomeTop10.count());

        Dataset<Row>  dsShopOfWelcomeTop10_2 = spark.sql("select (0.7*score/5+0.3*per_pay) as welcomeScore, * from shop_info where city_name='上海'  order by (0.7*score/5+0.3*per_pay) desc limit 10");
        System.out.println("^^^^^^^^-------3上海最受欢迎的10 家火锅商店编号-----");
        dsShopOfWelcomeTop10_2.show((int) dsShopOfWelcomeTop10_2.count());

        Dataset<Row>  dsShopOfWelcomeTop10_3 = spark.sql("select (0.7*score/5+0.3*per_pay) as welcomeScore, * from shop_info where city_name='广州'  order by (0.7*score/5+0.3*per_pay) desc limit 10");
        System.out.println("^^^^^^^^-------4广州最受欢迎的10 家火锅商店编号-----");
        dsShopOfWelcomeTop10_3.show((int) dsShopOfWelcomeTop10_3.count());


        spark.stop();
    }
}

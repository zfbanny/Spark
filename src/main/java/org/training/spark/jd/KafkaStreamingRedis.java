package org.training.spark.jd;


import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by banny on 2018/3/26.
 */
public class KafkaStreamingRedis {


    public static void main(String[] args) throws InterruptedException {
        //KafkaStreamingRedis streaming = new KafkaStreamingRedis();
        //streaming.processClick();

        SparkConf sparkConf = new SparkConf().setAppName("KafkaStreamingRedis").setMaster("local[1]");
        //两秒读取一次kafka
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

        //设置分割模式
        final Pattern SPACE = Pattern.compile(",");

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        kafkaParams.put("serializer.class", "kafka.serializer.StringEncoder");

        //话题名称以“，”分隔
        String topics = "t_click";
        //每个话题的分片数
        int numThreads = 1;

//        jssc.checkpoint("checkpoint"); //设置检查点
        //存放话题跟分片的映射关系
        Set<String> topicMap = new HashSet<>();
        topicMap.add("t_click");

        /**
         * 依次读取 kafka 中 t_click 主题数据，并统计: 每个页面累计点击次数，并存入redis，其中key为”click+<pid>”, value 为累计的次数
         * kafka数据格式: key:uid , value:click_time+”,”+pid
         *
         */
        JavaPairInputDStream<String, String> meaages = KafkaUtils.createDirectStream(jssc,
                String.class, String.class,
                kafka.serializer.StringDecoder.class, kafka.serializer.StringDecoder.class,
                kafkaParams,
                topicMap);

        //统计每个点击页 累计次数
        JavaPairDStream<String, Long> pageClicks = meaages.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> line) throws Exception {
                System.out.println("key:" + line._1() + " value: " + line._2());
                return new Tuple2<>(line._2().split(",")[1], Long.valueOf(1));
            }
        }).reduceByKey((x1, x2) -> (x1 + x2));

        //将数据放入Redis中,其中key:click+<pid>,value:累计的次数
        pageClicks.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            @Override
            public void call(JavaPairRDD<String, Long> stringLongJavaPairRDD) throws Exception {
                stringLongJavaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> partitionOfRecords) throws Exception {
                        while (partitionOfRecords.hasNext()) {
                            try {
                                String clickHashKey = "app::pid::click";
                                Tuple2<String, Long> pair = partitionOfRecords.next();
                                RedisClient.set(clickHashKey, "click"+pair._1().toString(), Long.parseLong(pair._2().toString()));
                                System.out.println("Update pageid " + pair._1() + " to " + pair._2().toString());
                            } catch (Exception e) {
                                System.out.println("error:" + e);
                            }
                        }
                    }
                });
            }
        });

        //打印结果
        pageClicks.print();
        jssc.start();
        jssc.awaitTermination();
    }

    public void processClick() throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("KafkaStreamingRedis").setMaster("local[1]");
        //两秒读取一次kafka
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

            //设置分割模式
            final Pattern SPACE = Pattern.compile(",");

            Map<String, String> kafkaParams = new HashMap<>();
            kafkaParams.put("metadata.broker.list", "localhost:9092");
            kafkaParams.put("serializer.class", "kafka.serializer.StringEncoder");

            //话题名称以“，”分隔
            String topics = "t_click";
            //每个话题的分片数
            int numThreads = 1;

//        jssc.checkpoint("checkpoint"); //设置检查点
            //存放话题跟分片的映射关系
            Set<String> topicMap = new HashSet<>();
            topicMap.add("t_click");

            /**
             * 依次读取 kafka 中 t_click 主题数据，并统计: 每个页面累计点击次数，并存入redis，其中key为”click+<pid>”, value 为累计的次数
             * kafka数据格式: key:uid , value:click_time+”,”+pid
             *
             */
            JavaPairInputDStream<String, String> meaages = KafkaUtils.createDirectStream(jssc,
                    String.class, String.class,
                    kafka.serializer.StringDecoder.class, kafka.serializer.StringDecoder.class,
                    kafkaParams,
                    topicMap);

            //统计每个点击页 累计次数
            JavaPairDStream<String, Long> pageClicks = meaages.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
                @Override
                public Tuple2<String, Long> call(Tuple2<String, String> line) throws Exception {
                    System.out.println("key:" + line._1() + " value: " + line._2());
                    return new Tuple2<>(line._2().split(",")[1], Long.valueOf(1));
                }
            }).reduceByKey((x1, x2) -> (x1 + x2));

            //将数据放入Redis中,其中key:click+<pid>,value:累计的次数
            pageClicks.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
                @Override
                public void call(JavaPairRDD<String, Long> stringLongJavaPairRDD) throws Exception {
                    stringLongJavaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                        @Override
                        public void call(Iterator<Tuple2<String, Long>> partitionOfRecords) throws Exception {
                            while (partitionOfRecords.hasNext()) {
                                try {
                                    String clickHashKey = "app::pid::click";
                                    Tuple2<String, Long> pair = partitionOfRecords.next();
                                    RedisClient.set(clickHashKey, "click"+pair._1().toString(),Long.parseLong(pair._2().toString()));
                                    System.out.println("Update pageid " + pair._1() + " to " + pair._2().toString());
                                } catch (Exception e) {
                                    System.out.println("error:" + e);
                                }
                            }
                        }
                    });
                }
            });

            //打印结果
            pageClicks.print();
            jssc.start();
            jssc.awaitTermination();


    }

    public void processOrder() throws InterruptedException {

        SparkConf sparkConf = new SparkConf().setAppName("KafkaStreamingRedis").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        //两秒读取一次kafka
        JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(2000));

        //设置分割模式
        final Pattern SPACE = Pattern.compile(",");

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        kafkaParams.put("serializer.class", "kafka.serializer.StringEncoder");

        //话题名称以“，”分隔
        String topics = "t_order";
        //每个话题的分片数
        int numThreads = 1;

//        jssc.checkpoint("checkpoint"); //设置检查点
        //存放话题跟分片的映射关系
        Set<String> topicMap = new HashSet<>();
        topicMap.add("t_order");

        /**
         * 依次读取 kafka 中 t_order 主题数据，并统计: 不同年龄段消费总金额，并存入redis，其中key为”buy+<age>”, value 为累计的消费金额
         *
         * kafka数据格式: key:uid， value:uid+”,”+price+“,”+discount
         *
         */
        JavaPairInputDStream<String, String> meaages = KafkaUtils.createDirectStream(jssc,
                String.class, String.class,
                kafka.serializer.StringDecoder.class, kafka.serializer.StringDecoder.class,
                kafkaParams,
                topicMap);

        //统计不同uid 消费总金额,输出格式: [uid,account]
        JavaPairDStream<String, Double> uidMoney = meaages.mapToPair(new PairFunction<Tuple2<String, String>, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, String> line) throws Exception {
                System.out.println("======line2===="+line._2());
                return new Tuple2<>(line._2().split(",")[0], Double.parseDouble(line._2().split(",")[1])-Double.parseDouble(line._2().split(",")[2]));
            }
        }).reduceByKey((x1, x2) -> (x1 + x2));


        JavaRDD<String> userRdd = sc.textFile("data/t_user.csv");

        //user RDD:[ uid , age]
        JavaPairRDD<String, String> users = userRdd.mapToPair(s ->
                new Tuple2<>(s.split(",")[0],s.split(",")[1]));
//
//            System.out.println("####:"+users.count());
        uidMoney.foreachRDD(new VoidFunction<JavaPairRDD<String, Double>>() {
            @Override
            public void call(JavaPairRDD<String, Double> stringLongJavaPairRDD) throws Exception {
                //transform to RDD:[uid,(account,age)]
                JavaPairRDD<String, Tuple2<Double, String>> join = stringLongJavaPairRDD.join(users);
                //transform to RDD:[age,account]
                JavaPairRDD<String, Double> ageAccount = join.mapToPair(j ->
                        new Tuple2<String, Double>(j._2()._2().toString(),j._2()._1()))
                        .reduceByKey((d1,d2) -> d1+d2);
                ageAccount.collect().forEach(t -> System.out.println("age:"+t._1() + ",account:"+t._2()));
            }
        });

        //打印结果
        uidMoney.print();
        jssc.start();
        jssc.awaitTermination();

    }
}



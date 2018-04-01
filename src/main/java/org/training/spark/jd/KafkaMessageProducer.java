package org.training.spark.jd;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by banny on 2018/3/26.
 */
public class KafkaMessageProducer {

    public void produceMessageClick(){

        //读取CSV文件
        File csv = new File("data/t_click1.csv");  // CSV文件路径
        BufferedReader br = null;
        try
        {
            br = new BufferedReader(new FileReader(csv));
        } catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
        String line = "";

        Properties props = getConfig();
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        try {
            List<String> allString = new ArrayList<>();

            while ((line = br.readLine()) != null)  //读取到的内容给line变量
            {
                //过滤第一行为uid标题的数据
                if (!line.startsWith("uid")) {
                    String[] click = line.split(",");
                    System.out.println(line);
                    allString.add(line);
                    //key:uid , value:click_time+”,”+pid
                    producer.send(new ProducerRecord<String, String>("t_click", click[0], click[1] + "," + click[2]));
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            producer.close();
            System.out.println("csv表格中所有行数："+allString.size());
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }
    public void produceMessageOrder(){

        //读取CSV文件
        File csv = new File("data/t_order1.csv");  // CSV文件路径
        BufferedReader br = null;
        try
        {
            br = new BufferedReader(new FileReader(csv));
        } catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
        String line = "";

        Properties props = getConfig();
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        try {
            List<String> allString = new ArrayList<>();

            while ((line = br.readLine()) != null)  //读取到的内容给line变量
            {
                    if(!line.startsWith("uid")){
                        String[] order = line.split(",");
                        System.out.println(line);
                        allString.add(line);
                        //数据格式: key:uid， value:uid+”,”+price+“,”+discount
                        producer.send(new ProducerRecord<String, String>("t_order", order[0],order[0]+","+order[2]+","+order[5]));
                        try {
                            Thread.sleep(10);
                        }
                        catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }


            }
            producer.close();
            System.out.println("csv表格中所有行数："+allString.size());
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public static class KafkaProducerRunner implements Runnable{
        private final Producer<String, String> producer;
        private final String cl;
        public  KafkaProducerRunner(String str){
            this.cl = str;
            Properties props = getConfig();
            producer = new KafkaProducer<String, String>(props);
        }

        public void run(){
            System.out.println("=====发送消息===="+cl);
            producer.send(new ProducerRecord<String, String>("t_click", "1","2"));
            System.out.println("=====结束消息===="+cl);
            producer.close();
        }

    }



    // config
    public static Properties getConfig()
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        /*  0: 不需要进行确认，速度最快。存在丢失数据的风险。
            1: 仅需要Leader进行确认，不需要ISR进行确认。是一种效率和安全折中的方式。
            all: 需要ISR中所有的Replica给予接收确认，速度最慢，安全性最高，但是由于ISR可能会缩小到仅包含一个Replica，所以设置参数为all并不能一定避免数据丢失。
        */
        props.put("acks", "0");
        props.put("retries", 0);
        props.put("batch.size", 1024);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public static void main(String[] args)
    {
        KafkaMessageProducer producer = new KafkaMessageProducer();
        producer.produceMessageClick();
        //producer.produceMessageOrder();
    }
}

package org.training.spark.jd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

/**
 * Created by banny on 2018/3/24.
 */
public class sparkJob4 {
    public static class Order implements Serializable {
        private String uid;
        private Date buy_time;
        private Double price;
        private int qty;
        private Double discount;

        public String getUid() {
            return uid;
        }

        public void setUid(String uid) {
            this.uid = uid;
        }

        public Date getBuy_time() {
            return buy_time;
        }

        public void setBuy_time(Date buy_time) {
            this.buy_time = buy_time;
        }

        public Double getPrice() {
            return price;
        }

        public void setPrice(Double price) {
            this.price = price;
        }

        public int getQty() {
            return qty;
        }

        public void setQty(int qty) {
            this.qty = qty;
        }

        public Double getDiscount() {
            return discount;
        }

        public void setDiscount(Double discount) {
            this.discount = discount;
        }
    }

    public static class Loan_sum implements Serializable{
        private String uid;
        private String month;
        private Double loan_sum;

        public String getUid() {
            return uid;
        }

        public void setUid(String uid) {
            this.uid = uid;
        }

        public String getMonth() {
            return month;
        }

        public void setMonth(String month) {
            this.month = month;
        }

        public Double getLoan_sum() {
            return loan_sum;
        }

        public void setLoan_sum(Double loan_sum) {
            this.loan_sum = loan_sum;
        }
    }

    public static StructType schema = new StructType()
            .add("uid", "string", false)
            .add("buy_time", "string", false)
            .add("price", "string", false)
            .add("qty", "string", false)
            .add("cate_id", "string", false)
            .add("discount", "string", false);


    public static StructType getOrderSchema(){
        String schemaString ="uid buy_time price qty cate_id discount";
        List<StructField> fields = new ArrayList<>();

        for(String fieldName : schemaString.split(" ")) {
            StructField field =
//            if(fieldName.equals("uid")){
//                field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
//            }else if(fieldName.equals("buy_time")){
//                field = DataTypes.createStructField(fieldName, DataTypes.DateType, true);
//            }else if(fieldName.equals("qty")){
//                field = DataTypes.createStructField(fieldName, DataTypes.IntegerType, true);
//            }else {
                field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            //}
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);
        return schema;
    }



    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();

        // 通过RDD加载csv文本数据
        //JavaRDD<Loan_sum> loan_sumJavaRDD = sparkSession.read().textFile("/Users/banny/Downloads/jd_data/t_loan_sum1.csv")
        JavaRDD<Loan_sum> loan_sumJavaRDD = sparkSession.read().textFile("data/t_loan_sum.csv")
        .toJavaRDD().map(new Function<String, Loan_sum>() {
                    @Override
                    public Loan_sum call(String str) throws Exception {
                        Loan_sum loan_sum = new Loan_sum();
                        String[] attributes = str.split(",");
                        loan_sum.setUid(attributes[0]);
                        loan_sum.setMonth(attributes[1]);
                        loan_sum.setLoan_sum(Double.parseDouble(attributes[2]));

                        return loan_sum;
                    }
                });
        // RDD转为DateFrame
        Dataset<Row> loansumDF = sparkSession.createDataFrame(loan_sumJavaRDD,Loan_sum.class).filter(col("uid").notEqual("uid"));
        loansumDF.printSchema();
        loansumDF.show(4);
        // DateFrame定义为临时表
        loansumDF.createOrReplaceTempView("t_loan_sum");

        //通过DateFrame加载csv文本数据
        Dataset<Row> orderDF = sparkSession.read().schema(schema).csv("data/t_order.csv");

        //Dataset<Row> orderDF = sparkSession.read().schema(schema).csv("/Users/banny/Downloads/jd_data/t_order1.csv");
        //过滤第一行数据
        Dataset<Row> order1DF = orderDF.filter(col("uid").notEqual("uid"));
        order1DF.show(4);
        order1DF.createOrReplaceTempView("t_order");

        // 借款金额超过 200 且购买商品总价值超过借款总金额的用户 ID
        System.out.println("=======SQL1输出:");
        sparkSession.sql("select a.u_id from (select t_loan_sum.uid as u_id,sum(price*qty-discount) as buy, sum(loan_sum) as loan " +
                "from t_loan_sum join t_order on t_loan_sum.uid = t_order.uid group by t_loan_sum.uid) a where a.loan > 200 and a.buy >a.loan").show();

        // 从不买打折产品且不借款的用户 ID
        System.out.println("=======SQL2输出:");
        sparkSession.sql("select uid from t_order group by uid having sum(discount) =0 and uid not in(select uid from t_loan_sum)").show();
        sparkSession.stop();
    }
}

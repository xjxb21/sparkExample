package com.xiao.spark;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

/**
 * Description: sparkSession
 * User: xiaojixiang
 * Date: 2018/2/12
 * Version: 1.0
 */

public class Example2 {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("Example2")
                .config("spark.master","local")
                .config("spark.executor.memory", "512m")
                .getOrCreate();

        //测试不支持标准JSON格式 ？？！
        Dataset<Row> df = spark.read().json("src/main/resources/people.json");
        df.show();

        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> r = df.filter((FilterFunction<Row>) value -> value.get(0) != null)
                .map((MapFunction<Row, String>) value -> value.getString(2), stringEncoder);

        r.show();
    }

}

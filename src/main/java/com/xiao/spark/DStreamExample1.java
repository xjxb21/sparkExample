package com.xiao.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Description:
 * User: xiaojixiang
 * Date: 2018/2/14
 * Version: 1.0
 */

public class DStreamExample1 {

    public static void main(String[] args) throws InterruptedException {

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("DStreamExample_1");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        //hive1利用nc工具开启9999端口
        JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("hive1", 9999);
        JavaDStream<String> words = lines.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairDStream<String, Integer> result = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        result.print();
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}

package com.xiao.spark;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * Description:
 * User: xiaojixiang
 * Date: 2018/2/16
 * Version: 1.0
 */

public class DStreamExample3 {
    public static void main(String[] args) throws InterruptedException {

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("DStreamExample_2");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        streamingContext.checkpoint(".");

        Map<String, String> kafkaParams = new HashMap();
        kafkaParams.put("metadata.broker.list", "hive1:9092,hive2:9092,hive3:9092");

        Set<String> topicSet = new HashSet<>();
        topicSet.add("sparkTestTop_2");

        JavaPairInputDStream<String, String> directStream = KafkaUtils.createDirectStream(streamingContext,
                String.class, String.class,
                StringDecoder.class, StringDecoder.class,
                kafkaParams, topicSet);

        JavaPairDStream<String, Integer> pair = directStream.mapToPair((PairFunction<Tuple2<String, String>, String, Integer>) stringStringTuple2 -> {
            // System.out.println(stringStringTuple2.toString());
            return new Tuple2<>(stringStringTuple2._2, 1);
        });

        //返回一个新的“状态”DStream，其中每个键的状态通过对键的先前状态和键的新值应用给定函数来更新。这可以用来维护每个键的任意状态数据。
        JavaPairDStream<String, Integer> pairDStream = pair.updateStateByKey((Function2<List<Integer>, Optional<Integer>, Optional<Integer>>) (values, state) -> {
            //第一个参数就是key传进来的数据，第二个参数是曾经已有的数据
            Integer update = state.orElse(0);
            for (Integer v : values) {
                update += v;
            }
            return Optional.of(update);
        });

        pairDStream.print();
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}

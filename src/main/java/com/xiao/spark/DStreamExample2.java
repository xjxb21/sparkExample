package com.xiao.spark;

import kafka.serializer.StringDecoder;
import kafka.serializer.StringEncoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Encoders;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Tuple2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Description: use kafka
 * <p>
 * ./kafka-topics.sh --list --zookeeper node3:2181
 * <p>
 * User: xiaojixiang
 * Date: 2018/2/16
 * Version: 1.0
 */

public class DStreamExample2 {

    public static void main(String[] args) throws InterruptedException {

        /***
         * use receiver
         */
/*
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("DStreamExample_2");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put("sparkTestTop_1", 2);
        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(streamingContext, "node3:2181,node4:2181,node5:2181",
                "sparkStreamGroup", topicMap, StorageLevel.MEMORY_ONLY());

        messages.map((Function<Tuple2<String, String>, String>) v1 -> {
            //输入字符串的kafka按行获取，key 为 null
            //---->(null,fasfd)
            System.out.println("---->" + v1.toString());
            return v1._2;
        }).print();

        streamingContext.start();
        streamingContext.awaitTermination();
*/

        /**
         * use directStream
         */
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("DStreamExample_3");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        Map<String,String> kafkaParams = new HashMap();
        kafkaParams.put("metadata.broker.list", "hive1:9092,hive2:9092,hive3:9092");

        Set<String> topicSet = new HashSet<>();
        topicSet.add("sparkTestTop_2");

        JavaPairInputDStream directStream = KafkaUtils.createDirectStream(streamingContext,
                String.class, String.class,
                StringDecoder.class, StringDecoder.class,
                kafkaParams, topicSet);

        AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

        directStream.transformToPair((Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>) v1 -> {
            OffsetRange[] offsets = ((HasOffsetRanges) v1.rdd()).offsetRanges();
            offsetRanges.set(offsets);
            return v1;
        }).foreachRDD((VoidFunction2<JavaPairRDD<String, String>, Time>) (v1, v2) -> {
            for (OffsetRange o : offsetRanges.get()) {
                System.out.println(
                        Thread.currentThread().getName() + " >>>> " + o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset()
                );
                //Thread.sleep(2000);
            }
        });

        streamingContext.start();
        streamingContext.awaitTermination();

    }
}

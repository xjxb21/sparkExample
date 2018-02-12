package com.xiao.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Description: spark version 2.2.1
 * User: xiaojixiang
 * Date: 2018/2/8
 * Version: 1.0
 */

public class SparkExample {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf();

//        String file = "hdfs://node2:8020/datas/README.txt";
//        sparkConf.setMaster("spark://hive1:7077").setJars(new String[]{"E:\\projects\\xiaoProjects\\sparkExample\\out\\artifacts\\sparkExample_jar\\sparkExample.jar"});

        //String file = "file:///C:\\Users\\xiao\\Desktop\\aaa.txt";
        String file = "hdfs://node2:8020/datas/README.txt";
        sparkConf.setMaster("spark://hive1:7077")
                .setJars(new String[]{"E:\\projects\\xiaoProjects\\sparkExample\\out\\artifacts\\sparkExample_jar\\sparkExample.jar"});

        SparkSession spark = SparkSession.builder().config(sparkConf)
                .config("spark.executor.memory", "512m")
                .appName("JavaWordCount")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(file).javaRDD();

        lines.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

//        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
//
//        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
//
//        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
//
//        List<Tuple2<String, Integer>> output = counts.collect();
//
//        for (Tuple2<?, ?> tuple : output) {
//            System.out.println(tuple._1() + ": " + tuple._2());
//        }

        spark.stop();
    }
}

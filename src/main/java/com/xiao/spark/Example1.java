package com.xiao.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * Description:
 * User: xiaojixiang
 * Date: 2018/2/11
 * Version: 1.0
 */

public class Example1 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();

        conf.setAppName("example1").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> rdd = sc.parallelize(data,4);

        System.out.println(rdd.take(2));

        Integer reduce = rdd.reduce((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        System.out.println(reduce);

        sc.close();
    }
}

package com.xiao.spark;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.spark.sql.*;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.sql.Date;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;

/**
 * Description: sparkSession
 * User: xiaojixiang
 * Date: 2018/2/12
 * Version: 1.0
 */

public class Example3 {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("Example3")
                .config("spark.master", "local")
                .config("spark.executor.memory", "512m")
                .getOrCreate();

        // Encoders are created for Java beans
        Encoder<WeatherFutureBean> encoder = Encoders.bean(WeatherFutureBean.class);
        Dataset<WeatherFutureBean> dataset = spark.createDataset(getWeatherData(), encoder);
//        dataset.show();

        dataset.createOrReplaceTempView("mytable");
        Dataset<Row> r1 = spark.sql("select * from mytable where high>11");
        r1.show();

        spark.close();
    }

    static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");


    public static List<WeatherFutureBean> getWeatherData() {
        List<WeatherFutureBean> result = new LinkedList<>();
        String city_id = "CHSH000000";
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        String httpUrl = "http://tj.nineton.cn/Heart/index/all?city=" + city_id;
        HttpGet httpGet = new HttpGet(httpUrl);
        try {
            CloseableHttpResponse httpResponse = httpClient.execute(httpGet);
            String v = EntityUtils.toString(httpResponse.getEntity());
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode node = objectMapper.readValue(v, JsonNode.class);

            JsonNode futureNode = node.get("weather").get(0).get("future");
            String City_name = node.get("weather").get(0).get("city_name").getTextValue();

            if (futureNode.isArray()) {
                futureNode.forEach(jsonNode -> {
                    WeatherFutureBean futureWeather = new WeatherFutureBean();
                    //.....
                    futureWeather.setCity_id("CHSH000000");
                    futureWeather.setCity_name(City_name);

                    //futureWeather.setFuture_date(LocalDate.parse(jsonNode.get("date").getTextValue(), formatter));
                    //恶心
                    long longDate = LocalDate.parse(jsonNode.get("date").getTextValue(), formatter)
                            .atStartOfDay().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                    futureWeather.setFuture_date(new Date(longDate));

                    futureWeather.setDay(jsonNode.get("day").getTextValue());
                    futureWeather.setText(jsonNode.get("text").getTextValue());
                    futureWeather.setHigh(Integer.parseInt(jsonNode.get("high").getTextValue()));
                    futureWeather.setLow(Integer.parseInt(jsonNode.get("low").getTextValue()));
                    futureWeather.setWind(jsonNode.get("wind").getTextValue());
                    result.add(futureWeather);
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return result;
        }
    }
}

package com.xiao.spark;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;

class Example3Test {

    static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Test
    void getJsonData() {
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

            //System.out.println(futureNode.toString());
            List<WeatherFutureBean> list = new LinkedList<>();
            if (futureNode.isArray()) {
                futureNode.forEach(jsonNode -> {
                    WeatherFutureBean futureWeather = new WeatherFutureBean();
                    //.....
                    futureWeather.setCity_id("CHSH000000");
                    futureWeather.setCity_name(City_name);
                    // futureWeather.setFuture_date(LocalDate.parse(jsonNode.get("date").getTextValue(),formatter));
                    futureWeather.setDay(jsonNode.get("day").getTextValue());
                    futureWeather.setText(jsonNode.get("text").getTextValue());
                    futureWeather.setHigh(Integer.parseInt(jsonNode.get("high").getTextValue()));
                    futureWeather.setLow(Integer.parseInt(jsonNode.get("low").getTextValue()));
                    futureWeather.setWind(jsonNode.get("wind").getTextValue());
                    list.add(futureWeather);
                });
            }
            System.out.println(list);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
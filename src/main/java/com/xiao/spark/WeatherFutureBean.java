package com.xiao.spark;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Description:
 * User: xiaojixiang
 * Date: 2018/2/13
 * Version: 1.0
 */

public class WeatherFutureBean implements Serializable {

    private String city_name;
    private String city_id;

    /**
     * Encoder 不支持java时间格式
     */
    //private LocalDateTime last_update;
    private java.sql.Timestamp last_update;

    //private LocalDate future_date;
    private java.sql.Date future_date;

    private String day; //周日
    private String text; //阴/小雨
    private int high; //23
    private int low; //18
    private String wind; //微风3级

    public Timestamp getLast_update() {
        return last_update;
    }

    public void setLast_update(Timestamp last_update) {
        this.last_update = last_update;
    }

    public Date getFuture_date() {
        return future_date;
    }

    public void setFuture_date(Date future_date) {
        this.future_date = future_date;
    }

    public String getCity_name() {
        return city_name;
    }

    public void setCity_name(String city_name) {
        this.city_name = city_name;
    }

    public String getCity_id() {
        return city_id;
    }

    public void setCity_id(String city_id) {
        this.city_id = city_id;
    }


    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public int getHigh() {
        return high;
    }

    public void setHigh(int high) {
        this.high = high;
    }

    public int getLow() {
        return low;
    }

    public void setLow(int low) {
        this.low = low;
    }

    public String getWind() {
        return wind;
    }

    public void setWind(String wind) {
        this.wind = wind;
    }
}

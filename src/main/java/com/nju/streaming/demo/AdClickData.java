package com.nju.streaming.demo;

/**
 * @description
 * @date:2022/11/19 20:47
 * @author: qyl
 */
public class AdClickData {
    public String ts;
    public String area;
    public String city;
    public String user;
    public String ad;

    public AdClickData(String ts, String area, String city, String user, String ad) {
        this.ts = ts;
        this.area = area;
        this.city = city;
        this.user = user;
        this.ad = ad;
    }

    public String getTs() {
        return ts;
    }

    public String getArea() {
        return area;
    }

    public String getCity() {
        return city;
    }

    public String getUser() {
        return user;
    }

    public String getAd() {
        return ad;
    }
}

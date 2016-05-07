package ro.workshop.spark.rdd.dto;

import java.io.Serializable;

public class Business implements Serializable {

    private String business_id;
    private String full_address;
    private boolean open;
    private String city;
    private Integer review_count;
    private String state;
    private String name;

    public String getBusiness_id() {
        return business_id;
    }

    public void setBusiness_id(String business_id) {
        this.business_id = business_id;
    }

    public String getFull_address() {
        return full_address;
    }

    public void setFull_address(String full_address) {
        this.full_address = full_address;
    }

    public boolean isOpen() {
        return open;
    }

    public void setOpen(boolean open) {
        this.open = open;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public Integer getReview_count() {
        return review_count;
    }

    public void setReview_count(Integer review_count) {
        this.review_count = review_count;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Business increaseReviewCountBy(Integer n) {
        review_count = review_count + n;
        return this;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    @Override
    public String toString() {
        return "Business{" +
                "business_id='" + business_id + '\'' +
                ", full_address='" + full_address.replaceAll("\\n", " ") + '\'' +
                ", open=" + open +
                ", city=" + city +
                ", review_count=" + review_count +
                ", state=" + state +
                ", name=" + name +
                '}';
    }
}

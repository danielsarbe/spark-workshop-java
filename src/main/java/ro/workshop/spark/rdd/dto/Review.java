package ro.workshop.spark.rdd.dto;

import java.io.Serializable;
import java.util.Date;

public class Review implements Serializable {

    private String review_id;
    private Date date;
    private String business_id;
    private Integer stars;


    public String getReview_id() {
        return review_id;
    }

    public void setReview_id(String review_id) {
        this.review_id = review_id;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public String getBusiness_id() {
        return business_id;
    }

    public void setBusiness_id(String business_id) {
        this.business_id = business_id;
    }

    public Integer getStars() {
        return stars;
    }

    public void setStars(Integer stars) {
        this.stars = stars;
    }

    @Override
    public String toString() {
        return "Review{" +
                "review_id='" + review_id + '\'' +
                ", date=" + date +
                ", business_id='" + business_id + '\'' +
                ", stars=" + stars +
                '}';
    }
}

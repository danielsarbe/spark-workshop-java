package ro.workshop.spark.rdd.dto;

import java.io.Serializable;
import java.util.Date;

public class User implements Serializable {

    private Date yelping_since;
    private Integer review_count;
    private String name;
    private String user_id;
    private Integer fans;

    public Date getYelping_since() {
        return yelping_since;
    }

    public void setYelping_since(Date yelping_since) {
        this.yelping_since = yelping_since;
    }

    public Integer getReview_count() {
        return review_count;
    }

    public void setReview_count(Integer review_count) {
        this.review_count = review_count;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public Integer getFans() {
        return fans;
    }

    public void setFans(Integer fans) {
        this.fans = fans;
    }

    @Override
    public String toString() {
        return "User{" +
                "yelping_since=" + yelping_since +
                ", review_count=" + review_count +
                ", name='" + name + '\'' +
                ", user_id='" + user_id + '\'' +
                ", fans=" + fans +
                '}';
    }
}

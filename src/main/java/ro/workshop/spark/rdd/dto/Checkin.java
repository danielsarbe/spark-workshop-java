package ro.workshop.spark.rdd.dto;

import java.io.Serializable;

public class Checkin implements Serializable {

    private String type;
    private String business_id;


    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getBusiness_id() {
        return business_id;
    }

    public void setBusiness_id(String business_id) {
        this.business_id = business_id;
    }

    @Override
    public String toString() {
        return "Checkin{" +
                "type='" + type + '\'' +
                ", business_id='" + business_id + '\'' +
                '}';
    }
}

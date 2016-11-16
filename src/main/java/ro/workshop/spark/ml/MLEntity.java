package ro.workshop.spark.ml;

import org.apache.spark.mllib.linalg.Vector;

import java.io.Serializable;


/**
 * Created by adrian.bona on 16/11/16.
 */
public class MLEntity implements Serializable {

    private String label;
    private Vector features;

    public MLEntity(String label, Vector features) {
        this.label = label;
        this.features = features;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Vector getFeatures() {
        return features;
    }

    public void setFeatures(Vector features) {
        this.features = features;
    }
}

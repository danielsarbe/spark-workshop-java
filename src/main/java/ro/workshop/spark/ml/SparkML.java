package ro.workshop.spark.ml;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;

public class SparkML {

    public static class  Entity implements Serializable {
        private String id;
        private Vector features;

        public Entity(String id, Vector features) {
            this.id = id;
            this.features = features;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public Vector getFeatures() {
            return features;
        }

        public void setFeatures(Vector features) {
            this.features = features;
        }
    }
    private static void geoClusterBusinesses(SQLContext sqlContext) {
        DataFrame places = sqlContext.sql("select business_id as id, latitude, longitude from businesses");

        JavaRDD<Entity> rdd = places.toJavaRDD().map(row -> new Entity(row.getString(0), Vectors.dense(row.getDouble(1), row.getDouble(2))));

        DataFrame dataFrame = sqlContext.createDataFrame(rdd, Entity.class);

        KMeans kmeans = new KMeans().setK(10).
                setMaxIter(20).
                setFeaturesCol("features").
                setPredictionCol("prediction");
        KMeansModel model = kmeans.fit(dataFrame);

        // Shows the result
        Vector[] centers = model.clusterCenters();
        System.out.println("Cluster Centers: ");
        for (Vector center : centers) {
            System.out.println(center);
        }

        DataFrame transform = model.transform(dataFrame);
        transform.groupBy(transform.col("prediction")).count().show();

    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JavaKMeansExample").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        String inputPathBusiness = "data/s_business.json";
        DataFrame inputBusiness = sqlContext.read().json(inputPathBusiness);
        inputBusiness.registerTempTable("businesses");

        geoClusterBusinesses(sqlContext);

        jsc.stop();
    }
}

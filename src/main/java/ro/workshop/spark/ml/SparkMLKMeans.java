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


public class SparkMLKMeans {

    private static void behaviourClusterUsers(SQLContext sqlContext) {
        DataFrame places = sqlContext.sql("select user_id as id, review_count from user");

        JavaRDD<MLEntity> rdd = places.toJavaRDD().map(row -> new MLEntity(row.getString(0), Vectors.dense(row.getLong(1))));

        DataFrame dataFrame = sqlContext.createDataFrame(rdd, MLEntity.class);

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

    private static void geoClusterBusinesses(SQLContext sqlContext) {
        //TODO - Implement KMeans for latitude, longitude

    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JavaKMeansExample").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        String inputPathUser = "data/s_user.json";
        String inputPathBusiness = "data/s_business.json";

        DataFrame inputUser = sqlContext.read().json(inputPathUser);
        DataFrame inputBusiness = sqlContext.read().json(inputPathBusiness);

        inputUser.registerTempTable("user");
        inputBusiness.registerTempTable("business");

        behaviourClusterUsers(sqlContext);

        //TODO - Implement KMeans for latitude, longitude
        geoClusterBusinesses(sqlContext);

        jsc.stop();
    }
}

package ro.workshop.spark.ml;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.List;


public class SparkMLDT {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("JavaKMeansExample")
                .setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);


        final DataFrame locationDataFrame = buildLocationDataFrame(sqlContext);
        locationDataFrame.show();

        classifyLocations(locationDataFrame);
        jsc.stop();
    }

    public static DataFrame buildLocationDataFrame(SQLContext sqlContext) {

        // load initial data and filter only that is of interest
        final DataFrame businessDF = sqlContext
                .read()
                .json("data/yelp_academic_dataset_business.json");

        final DataFrame cityLatLong =
                businessDF.select(businessDF.col("city"), businessDF.col("latitude"), businessDF.col("longitude"));


        // find some representative cities
        final List<String> top5 = findTop5Cities(cityLatLong);

        final JavaRDD<MLEntity> businessMLRDD = businessDF
                .filter(businessDF
                        .col("city")
                        .isin(top5.toArray()))
                .javaRDD()
                .map(row -> new MLEntity(row.<String>getAs("city"),
                        Vectors.dense(row.<Double>getAs("latitude"), row.<Double>getAs("longitude"))));

        return sqlContext.createDataFrame(businessMLRDD, MLEntity.class);
    }

    private static List<String> findTop5Cities(DataFrame cityLatLong) {
        final DataFrame cityCounts = cityLatLong
                .groupBy(cityLatLong.col("city"))
                .count();
        final DataFrame cityTop = cityCounts.orderBy(cityCounts
                .col("count")
                .desc());
        cityTop.show();

        return cityTop
                .limit(5)
                .toJavaRDD()
                .map(row -> row.<String>getAs("city"))
                .collect();
    }

    private static void classifyLocations(DataFrame locationDF) {
        // city names => numeric
        final StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol("label")
                .setOutputCol("indexedLabel")
                .fit(locationDF);

        // classifier definition
        final DecisionTreeClassifier dt = new DecisionTreeClassifier()
                .setLabelCol("indexedLabel")
                .setFeaturesCol("features");

        // numeric => city names
        final IndexToString labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());

        // glue everything
        final Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { labelIndexer, dt, labelConverter });

        final DataFrame[] splits = locationDF.randomSplit(new double[] { 0.7, 0.3 });
        final DataFrame trainingSet = splits[0];
        final DataFrame testSet = splits[1];

        final PipelineModel model = pipeline.fit(trainingSet);

        final DataFrame predictions = model.transform(testSet);

        predictions
                .select(predictions.col("predictedLabel"), predictions.col("label"), predictions.col("features"))
                .show(5);

        final MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("indexedLabel")
                .setPredictionCol("prediction")
                .setMetricName("precision");

        final double accuracy = evaluator.evaluate(predictions);

        System.out.println();
        System.out.println("Accuracy = " + accuracy);
        System.out.println("Test Error = " + (1.0 - accuracy));

        System.out.println("Learned classification tree model:\n"
                + ((DecisionTreeClassificationModel) model.stages()[1]).toDebugString());
    }
}
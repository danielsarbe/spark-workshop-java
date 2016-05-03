package ro.workshop.spark;

import java.io.File;
import java.util.Arrays;

import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.Function;
import ro.workshop.spark.dto.Business;
import scala.Tuple2;


/**
 * Created by danielsarbe on 24/04/16.
 * This is a Spark class that presents the main transformations and actions available in Spark.
 * The examples are made using the Yelp database.
 * Uncomment the lines and compleate them with the right example as the presentation slieds advence.
 */

public class SparkTransformationsAndActions {

    public static void main(String[] args) throws Exception {

        System.out.println(System.getProperty("hadoop.home.dir"));

        String inputPath = "yelp/yelp_academic_dataset_business.json";

        /**
         * The first thing a Spark program must do is to create a JavaSparkContext object,
         * which tells Spark how to access a cluster.
         *
         *  1- The AppName will be shown in the cluster UI: Mesos, Spark, ot YARN
         *  2- The master is the name of the machine, we use local if the user run the program in a local machine
         *  3- A property of the number of cores to be use by the software
         */


        SparkConf conf = new SparkConf().setAppName("word-counter").setMaster("local").set("spark.cores.max", "4");
        JavaSparkContext sc = new JavaSparkContext(conf);


        //1 - Create RDD and read the JSON file
        JavaRDD<Business> businessRDD = sc.textFile(inputPath).map(
                new Function<String, Business>() {
                    public Business call(String line) throws Exception {
                        Gson gson = new Gson();
                        Business business = gson.fromJson(line, Business.class);
                        return business;
                    }
                });

        //2.1 How many records contain the business RDD ?
        System.out.println("No of business: <" + businessRDD.count() + ">");

        //TODO - 2.1 Print the number of records for the others RDDs


        //2.2 Find out the number of business from each city

        //TODO - 2.2 Which are the top 10 cities based on number of businesses ?

        //2.3 Find our number of reviews from each day

        //TODO - 2.3 Which are the top 10 days based on number of reviews?
        //TODO - 2.3 What is the number of reviews for each star?



        //Select a small number of samples that we can track easily
        JavaRDD<Business> sampleRDDs = businessRDD.sample(false, 0.001, 1);
        System.out.println("Selected samples: <" + sampleRDDs.count() + ">");

        //collect()
        for (Business business : sampleRDDs.collect()) {
            System.out.println(business.toString());
        }

        //cache()?
        JavaRDD<Business> newSampleRDD = sampleRDDs.map(b -> b.increaseReviewCountBy(1));
        System.out.println("Selected samples: <" + newSampleRDD.count() + ">");
        for (Business business : newSampleRDD.collect()) {
            System.out.println(business.toString());
        }


        //Find the number of business per State
        JavaPairRDD<String, Integer> pairs = businessRDD.mapToPair(b -> new Tuple2(b.getState(), 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);

        System.out.println("Distribution by state:");
        System.out.println(counts.collect());

        //What dose this returns?
        JavaRDD<String> addrPairs = businessRDD.map(b -> b.getFull_address());

        JavaPairRDD<String, Integer> pairRDD = addrPairs.flatMap(line -> Arrays.asList(line.replaceAll("\\n", " ").split("[,\\s]+")))
                .mapToPair(w -> new Tuple2<String, Integer>(w, 1))
                .reduceByKey((x, y) -> x + y)
                .filter((x) -> x._2() > 1000)
                .coalesce(1, true)
                .sortByKey();

        //System.out.println("pairRDD:" + pairRDD.collect());

        String outputPath = "out/pairRDD/";
        FileUtils.deleteQuietly(new File(outputPath));

        pairRDD.saveAsTextFile(outputPath);


        sc.close();


    }


}
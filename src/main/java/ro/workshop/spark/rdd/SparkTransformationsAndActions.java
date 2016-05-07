package ro.workshop.spark.rdd;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.Function;
import ro.workshop.spark.rdd.dto.Business;
import ro.workshop.spark.rdd.dto.Checkin;
import ro.workshop.spark.rdd.dto.Review;
import ro.workshop.spark.rdd.dto.User;
import scala.Tuple2;


/**
 * This is a Spark class that presents the main transformations and actions available in Spark.
 * The examples are made using the Yelp database.
 * Uncomment the lines and compleate them with the right example as the presentation slieds advence.
 */

public class SparkTransformationsAndActions {

    private final static Logger logger = Logger.getLogger(SparkTransformationsAndActions.class);
    private static final SimpleDateFormat dateFormatYearMonth = new SimpleDateFormat("yyyy-MM");

    public static void main(String[] args) throws Exception {

        /**
         For development/testing, select only 2k lines from each file
         linux: head -n 2000 f1.json > f1.json1
         windows: more +2000 file (to try)
         */
        String inputPathB = "data/?_business.json";
        String inputPathC = "data/?_checkin.json";
        String inputPathR = "data/?_review.json";
        String inputPathU =  "data/?_user.json";


        SparkConf conf = new SparkConf().setAppName("SparkWorkshop").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);


        //1 - Create RDD and read the JSON file
        JavaRDD<Business> businessRDD = sc.textFile(inputPathB).map(
                new Function<String, Business>() {
                    public Business call(String line) throws Exception {
                        Gson gson = new Gson();
                        return gson.fromJson(line, Business.class);
                    }
                });

        JavaRDD<Checkin> checkinsRDD = sc.textFile(inputPathC).map(
                new Function<String, Checkin>() {
                    public Checkin call(String line) throws Exception {
                        Gson gson = new Gson();
                        return gson.fromJson(line, Checkin.class);
                    }
                });

        JavaRDD<Review> reviewsRDD = sc.textFile(inputPathR).map(
                new Function<String, Review>() {
                    public Review call(String line) throws Exception {
                        Gson gson = new Gson();
                        return gson.fromJson(line, Review.class);
                    }
                });

        JavaRDD<User> usersRDD = sc.textFile(inputPathU).map(
                new Function<String, User>() {
                    public User call(String line) throws Exception {
                        Gson gson = new GsonBuilder().setDateFormat("yyyy-MM").create();
                        return gson.fromJson(line, User.class);
                    }
                });

        //2.1 How many records contain the business RDD ?
        logger.info("No of business: <" + businessRDD.count() + ">");

        //TODO - 2.1 Print the number of records for the others RDDs
        logger.info("No of checkins: <" + checkinsRDD.count() + ">");
        logger.info("No of reviews: <" + reviewsRDD.count() + ">");
        logger.info("No of users: <" + usersRDD.count() + ">");


        //2.2 Find out the number of business from each city. ??? Print the first 30
        JavaPairRDD<String, Iterable<Business>> businessGroupedByCity = businessRDD.groupBy(x -> x.getCity().trim());

        //TODO - 2.2 Which are the top 10 cities based on number of businesses ?
        JavaPairRDD<String, Integer> citiesRDD = businessGroupedByCity.mapToPair(city -> new Tuple2<>(city._1(), Iterators.size(city._2().iterator())));
        JavaPairRDD<String, Integer> citiesOrderByNumberOfBusiness = citiesRDD.mapToPair(a -> a.swap()).sortByKey(false).mapToPair(a -> a.swap());


        List<Tuple2<String, Integer>> top10CitiesRDD = citiesOrderByNumberOfBusiness.take(10);
        logger.info(" ==== Top 10 cities ==== ");
        logger.info(top10CitiesRDD);


        //2.3 Find our number of reviews from each day
        JavaPairRDD<Date, Iterable<Review>> reviewsGroupedByDateRDD = reviewsRDD.groupBy(x -> x.getDate());
        logger.info("groupByDateRDD: <" + reviewsGroupedByDateRDD.take(100) + ">");

        //TODO - 2.3 Which are the top 10 days based on number of reviews?
        List top10Days = reviewsGroupedByDateRDD
                .mapToPair(city -> new Tuple2<>(city._1(), Iterators.size(city._2().iterator())))
                .takeOrdered(10, (SerializableComparator<Tuple2<Date, Integer>>) (o1, o2) -> o2._2().compareTo(o1._2()));

        logger.info(" ==== Top 10 days ==== ");
        logger.info(top10Days);

        //TODO - 2.3 What is the number of reviews for each star?
        List reviewsByStar = reviewsRDD.groupBy(x -> x.getStars())
                .mapToPair(review -> new Tuple2<>(review._1(), Iterators.size(review._2().iterator())))
                .takeOrdered(10, (SerializableComparator<Tuple2<Integer, Integer>>) (o1, o2) -> o2._2().compareTo(o1._2()));

        logger.info(" ==== Reviews stars ==== ");
        logger.info(reviewsByStar);


        //2.4 Find out the first registered users
        JavaRDD<User> usersSortedByDate = usersRDD.sortBy(new Function<User, Date>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Date call(User value) throws Exception {
                return value.getYelping_since();
            }
        }, true, 1);

        logger.info(" ==== Yelping since ==== ");
        logger.info(usersSortedByDate.first());

        //TODO : In which months was registered the most users ?
        JavaPairRDD<Date, Integer> registeredUsersPerMonth = usersSortedByDate.mapToPair(w -> new Tuple2<Date, Integer>(w.getYelping_since(), 1)).
                reduceByKey((x, y) -> x + y);

        JavaPairRDD<Date, Integer> mostActiveMonths = registeredUsersPerMonth.mapToPair(a -> a.swap()).sortByKey(false).mapToPair(a -> a.swap());

        logger.info(" ==== Most Active Month ==== ");
        logger.info(dateFormatYearMonth.format(mostActiveMonths.first()._1()) + " : " + mostActiveMonths.first()._2());

        //2.5 Retrieve all the reviews for each business [grouped by business, so that is more easy to do exercises?]
        JavaPairRDD<String, Business> businessPair = businessRDD.mapToPair(business -> new Tuple2<>(business.getBusiness_id(), business));
        JavaPairRDD<String, Review> reviewsPair = reviewsRDD.mapToPair(review -> new Tuple2<>(review.getBusiness_id(), review));
        JavaPairRDD<String, Tuple2<Business, Optional<Review>>> joinedBusinessAndReviews = businessPair.leftOuterJoin(reviewsPair);
        logger.info(joinedBusinessAndReviews.count());

        //TODO : Retrieve all the reviews of “Red White & Brew” business
        //Var1
        JavaPairRDD<String, Tuple2<Business, Optional<Review>>> filteredReviews = joinedBusinessAndReviews.filter((x) -> x._2()._1().getName().equals("Clancy's Pub"));
        logger.info("filteredReviews:<" + filteredReviews.collect() + ">");
        //Var2
        String businessId = businessRDD.filter((x) -> x.getName().equals("Clancy's Pub")).collect().get(0).getBusiness_id();
        logger.info("businessId:<" + businessId + ">");
        JavaPairRDD<String, Tuple2<Business, Optional<Review>>> filteredReviews2 = joinedBusinessAndReviews.filter((x) -> x._1().equals(businessId));
        logger.info("filteredReviews2:<" + filteredReviews2.collect() + ">");

        //joinedBusinessAndReviews.take(10).saveAsTextFile(outputPath);

        //TODO : Retrieve all the checkins for each business
        JavaPairRDD<String, Checkin> checkinsPair = checkinsRDD.mapToPair(checkin -> new Tuple2<>(checkin.getBusiness_id(), checkin));
        JavaPairRDD<String, Tuple2<Business, Optional<Checkin>>> joinedBusinessAndCheckins = businessPair.leftOuterJoin(checkinsPair);
        logger.info(" ==== No of joined checkins ==== ");
        logger.info(joinedBusinessAndCheckins.count());

        sc.close();


    }


}

interface SerializableComparator<T> extends Comparator<T>, Serializable {
    @Override
    int compare(T o1, T o2);
}
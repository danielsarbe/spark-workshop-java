package ro.workshop.spark.sql;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class SparkSQL {

    private final static Logger logger = Logger.getLogger(SparkSQL.class);

    public static void main(String[] args) {

        String inputPathBusiness = "data/s_business.json";
        String inputPathCheckin = "data/s_checkin.json";
        String inputPathReview = "data/s_review.json";
        String inputPathUser =  "data/s_user.json";

        SparkConf sparkSQL = new SparkConf().setAppName("SparkSQL").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSQL);
        SQLContext sqlContext = new SQLContext(javaSparkContext);

        DataFrame inputBusiness = sqlContext.read().json(inputPathBusiness);
        DataFrame inputCheckin = sqlContext.read().json(inputPathCheckin);
        DataFrame inputReview = sqlContext.read().json(inputPathReview);
        DataFrame inputUser = sqlContext.read().json(inputPathUser);

        // Print the schema
        //inputBusiness.printSchema();

        // Register the input schema RDD
        inputBusiness.registerTempTable("business");
        inputCheckin.registerTempTable("checkin");
        inputReview.registerTempTable("review");
        inputUser.registerTempTable("user");

        //2.1 How many records contain the business RDD
        DataFrame business = sqlContext.sql("SELECT * FROM business");
        logger.info("No of business: <" + business.count() + ">");

        // 2.2 Find out the number of businesses from each city.
        sqlContext.sql("select city, count(*) from business group by city").show();

        // TODO - 2.2 Which are the top 10 cities based on number of businesses ?
        sqlContext.sql("select city, count(*) as c from business group by city order by c desc").show();

        // 2.3 Find our number of reviews from each day
        sqlContext.sql("select date, count(*) from review group by date");
        // TODO - 2.3 Which are the top 10 days based on number of reviews?
        // TODO - 2.3 What is the number of reviews for each star?

        // 2.4 Find out the first registered user
        sqlContext.sql("select * from user order by yelping_since desc limit 1").show();
        // TODO - 2.4 In which months was registered the most users ?

        // 2.5 Retrieve all the reviews for each business name
        sqlContext.sql("select b.name, count(*) from business b join review r on b.business_id = r.business_id group by b.name").show();
        // TODO - 2.5 Retrieve all the reviews of “Red White & Brew” business
        // TODO - 2.5 Retrieve all the checkins for each business


        javaSparkContext.stop();
    }
}

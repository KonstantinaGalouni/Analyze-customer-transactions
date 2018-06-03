import org.apache.spark.sql.*;

public class Main {
    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                //.master("spark://astero.di.uoa.gr:7077")
                .appName("recommendations")
                .getOrCreate();
        
        String events_csv = "/home/konstantina/Documents/big_data/retailrocket-recommender-system-dataset/events.csv";
        String category_tree_csv = "/home/konstantina/Documents/big_data/retailrocket-recommender-system-dataset/category_tree.csv";

        Dataset events = spark.read().option("header", "true").csv(events_csv);
        Dataset category_tree = spark.read().option("header", "true").csv(category_tree_csv);

        //String events_avro = "hdfs://astero.di.uoa.gr:8020/topics/events";
        //String category_tree_avro = "hdfs://astero.di.uoa.gr:8020/topics/categories";

        //Dataset events = spark.read().format("com.databricks.spark.avro").load(events_avro);
        //Dataset category_tree = spark.read().format("com.databricks.spark.avro").load(category_tree_avro);
        //Dataset items = spark.read().format("com.databricks.spark.avro").load("hdfs://astero.di.uoa.gr:8020/topics/items");

        ALS_Recommendation als = new ALS_Recommendation(spark, events);
        //als.createVisitorItemRatings();
        //als.printVisitorItemRatings();
        //als.createRatingsFile();
        //long start = System.nanoTime();
        //als.ALS_Cross_Val(5);
        //long elapsedTime = System.nanoTime() - start;
        //System.out.println("Elapsed Time = "+(double)elapsedTime/1000000000 + " seconds");
        //als.saveAndLoadModel();


        // Some minor statistics with Java Spark - testing purposes

        //events.createTempView("Events");
        //Statistics stats = new Statistics(spark, events, category_tree);
        //stats.countActions();
        //stats.categoriesByParent();
        //stats.findRootCategories();

        spark.stop();
    }
}

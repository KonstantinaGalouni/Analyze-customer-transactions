import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.from_unixtime;

public class Statistics {
    SparkSession spark;

    Dataset events;
    Dataset category_tree;

    Statistics(SparkSession spark, Dataset events, Dataset category_tree) {
        this.spark = spark;
        this.events = events;
        this.category_tree = category_tree;

        try {
            this.category_tree.createTempView("Category_tree");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }

    void countActions() {
        //events.show();

        this.spark.sql("select" +
                "        count(*) as Total_Actions," +
                "        count(case when event = 'view' then 1 end) as Views,\n" +
                "        count(case when event = 'addtocart' then 1 end) as AddToCart,\n" +
                "        count(case when event = 'transaction' then 1 end) as Transactions\n" +
                "        from Events").show();

    }

    void transactionsToOtherActions() {
        // where visitorid=151138 -> transactions = 1, addtocart = 0
        Dataset statistics = this.spark.sql("select" +
                "       *, Transactions/Total as Transactions_To_Total, Transactions/AddToCart as Transactions_To_AddToCart" +
                "       from ( " +
                "       select " +
                "       visitorid," +
                "       count(*) as Total," +
                "       count(case when Events.event = 'view' then 1 end) as Views,\n" +
                "       count(case when Events.event = 'addtocart' then 1 end) as AddToCart,\n" +
                "       count(case when Events.event = 'transaction' then 1 end) as Transactions\n" +
                "       from Events" +
                "       Group By visitorid" +
                "       Order By Transactions DESC) s");
        statistics.show();
        //statistics.where("Transactions_To_Total > 0.5").show();
        //statistics.where("Transactions > 10").show();

        long visitorsNum = statistics.count();
        System.out.println("Number of visitors = "+visitorsNum);
    }

    void timestampToDate() {
        Dataset eventsWithDate = this.events.withColumn("date", from_unixtime(events.col("timestamp").divide(1000))).drop("timestamp");
        eventsWithDate.show();
    }

    void categoriesByParent() {
        long categoriesNum = this.category_tree.distinct().count();
        //System.out.println("Number of categories = "+categoriesNum);

        Dataset categoriesGroupedByParent = this.spark.sql(" select categoryid from Category_tree where parentid is not null group By categoryid, parentid order by parentid");
        categoriesGroupedByParent.show();
        //System.out.println(categoriesGroupedByParent.collectAsList());
    }

    void findRootCategories() {
        //Dataset parentCategories = this.category_tree.select("parentid").distinct();
        //parentCategories.show();

        Dataset rootCategories = this.category_tree.select("categoryid").where("parentid is null").distinct();
        rootCategories.coalesce(1).write().mode(SaveMode.Overwrite).option("header", "true").csv("/home/konstantina/Documents/rootCategories.csv");
        rootCategories.show();
        long rootCategoriesNum = rootCategories.count();
        System.out.println("Number of categories without parent = "+rootCategoriesNum);

    }
}

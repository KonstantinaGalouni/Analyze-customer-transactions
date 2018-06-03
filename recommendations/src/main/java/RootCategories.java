/*JAVA code to get the root category of each category id*/

import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class RootCategories {
    SparkSession spark;

    Dataset events;
    Dataset category_tree;

    String path_for_exported_stats;

    RootCategories(SparkSession spark, Dataset events, Dataset category_tree, String path_for_exported_stats) {
        this.spark = spark;
        this.events = events;
        this.category_tree = category_tree;
        this.path_for_exported_stats = path_for_exported_stats;
    }

    void getRootCategories() throws AnalysisException {
        //create a list of categoryids
        List<String> cats = spark.sql("SELECT categoryid FROM categories").as(Encoders.STRING()).collectAsList();

        //create a new list to store the results
        List<Row> cats_and_parents_list = new ArrayList<>();

        //find the root category of each categoryid and add it to the cats_and_parents_list
        int i;
        String curParent;            //current parent
        String temp;                //current query result
        List<String> tempList;        //current result fields (categoryid-parentid) as list
        for (i = 0; i < cats.size(); i++) {
            //initialize parent variable
            curParent = cats.get(i);

            //get the first parent
            temp = spark.sql("SELECT parentid FROM categories WHERE categoryid LIKE " + curParent).as(Encoders.STRING()).collectAsList().get(0);

            //if empty then current category is a root category
            //and will be added as a parent of itself
            while (temp != null && !temp.isEmpty()) {
                curParent = temp;
                temp = spark.sql("SELECT parentid FROM categories WHERE categoryid LIKE " + curParent).as(Encoders.STRING()).collectAsList().get(0);
            }
            //create a list with the current result row values
            tempList = new ArrayList<>();
            tempList.add(cats.get(i));
            tempList.add(curParent);

            //add a new row to the results list
            cats_and_parents_list.add(RowFactory.create(tempList.toArray()));
        }

        //create a schema of the results
        StructType results_schema = new StructType();
        results_schema = results_schema.add("categoryid", DataTypes.StringType, true);
        results_schema = results_schema.add("ancestorid", DataTypes.StringType, true);

        //create a corresponding encoder
        ExpressionEncoder<Row> encoder = RowEncoder.apply(results_schema);

        //pass the results to a dataset
        Dataset<Row> exported = spark.createDataset(cats_and_parents_list, encoder);
        exported.show(5);

        //export to csv
        //note a folder will be created at the given path, the .csv will be stored inside
        exported.coalesce(1).write().format("com.databricks.spark.csv").option("header", "true").save(this.path_for_exported_stats+"ancestor_per_catid");

        //check the number of distinct ancestorids (there should be 25)
        exported.createTempView("ancestors");
        spark.sql("SELECT count(distinct ancestorid) as NoOdAncestors from ancestors").show();
    }
}

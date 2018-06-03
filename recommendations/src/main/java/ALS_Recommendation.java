import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

class ItemRating {
    Integer item;
    Double rating;
    Double prediction;
}

public class ALS_Recommendation {
    SparkSession spark;
    Dataset events;

    JavaPairRDD<Tuple2<String, String>, Double> visitor_item_ratings;
    JavaRDD<Rating> ratings;

    MatrixFactorizationModel model;

    ALS_Recommendation(SparkSession spark, Dataset events) {
        this.spark = spark;
        this.events = events;
    }


    void createVisitorItemRatings() {
        JavaRDD<Row> rdd = this.events.toJavaRDD();
        //StructType schema = this.events.schema();
        //Dataset<Row> dataset = spark.createDataFrame(rdd, schema);


        JavaPairRDD<Tuple2<String, String>, Double> visitor_item_ratings = rdd.map(line -> line.toString().split(",")).mapToPair(line -> {
            Tuple2<String, String> visitor_item = new Tuple2<>(line[1], line[3]);

            if (line[2].equals("view")) {
                return new Tuple2<Tuple2<String, String>, Double>(visitor_item, 0.3);
            } else if (line[2].equals("addtocart")) {
                return new Tuple2<Tuple2<String, String>, Double>(visitor_item, 0.7);
            } else {
                return new Tuple2<Tuple2<String, String>, Double>(visitor_item, 1.0);
            }
        })
                .reduceByKey((a, b) -> a + b);

        this.visitor_item_ratings = visitor_item_ratings;
    }

    void printVisitorItemRatings() {
        this.visitor_item_ratings.take(20).forEach(s -> {
            Tuple2<String, String> key = s._1;
            System.out.println("visitor: " + key._1 + ", item: " + key._2 + " - " + s._2);
        });
    }

    void createRatingsFile() {
        JavaRDD<Rating> ratings = this.visitor_item_ratings.map(s -> {
            Tuple2<String, String> key = s._1;
            return new Rating(Integer.parseInt(key._1), Integer.parseInt(key._2), s._2);
        });
        this.ratings = ratings;
    }

    void saveAndLoadModel() {
        SparkContext sc = this.spark.sparkContext();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

        // Save and load model
        this.model.save(jsc.sc(), "target/tmp/myCollaborativeFilter");
        MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(jsc.sc(),
                "target/tmp/myCollaborativeFilter");
    }


    JavaRDD<Rating>[] randomSplitRatings(JavaRDD<Rating> events, double weight1, double weight2) {
        JavaRDD<Rating>[] partEvents = events.randomSplit(new double[]{weight1, weight2});
        return partEvents;
    }

    JavaRDD<Rating>[] randomSplitTrainingRatings(JavaRDD<Rating> training, int folds) {
        double[] weights = new double[folds];
        double weight = 1.0/folds;
        for(int i=0; i<folds; i++) {
            weights[i] = weight;
        }
        JavaRDD<Rating>[] partTraining = training.randomSplit(weights);

        return partTraining;
    }

    void ALS_Cross_Val(int folds) {
        MatrixFactorizationModel model = null;
        double bestPercentileRanking = Double.MAX_VALUE;
        int bestRank = 0;
        int bestNumIter = 0;
        double bestLambda = 0.0;
        double bestAlpha = 0.0;

        long start;
        long elapsedTime;

        createVisitorItemRatings();
        createRatingsFile();

        JavaRDD<Rating>[] randomSplitRatings = randomSplitRatings(ratings, 0.8, 0.2);
        JavaRDD<Rating> train_and_validation_set = randomSplitRatings[0];
        train_and_validation_set.cache();
        JavaRDD<Rating> test_set = randomSplitRatings[1];

        JavaRDD<Rating>[] randomSplitTrainingRatings = randomSplitTrainingRatings(train_and_validation_set, folds);
        JavaRDD<Rating> train_set;
        JavaRDD<Rating> validation_set;

        int[] ranks = new int[]{20};//, 5, 15, 100};
        int[] iterations = new int[]{20};//, 15};
        double[] lamdas = new double[]{0.1};//, 0.5, 1.0, 10.0};
        double[] alphas = new double[]{0.5};//, 0.1, 0.9};

        for(int rank : ranks) {
            for(int iteration : iterations) {
                for(double lamda : lamdas) {
                    for(double alpha : alphas) {

                        double[] percentileRankings = new double[folds];
                        double sum = 0.0;

                        for(int fold=0; fold<folds; fold++) {
                            train_set = null;
                            validation_set = null;

                            for(int i=0; i<folds; i++) {
                                if(i != fold) {
                                    if(train_set == null) {
                                        train_set = randomSplitTrainingRatings[i];
                                    } else {
                                        train_set = train_set.union(randomSplitTrainingRatings[i]);
                                    }
                                } else {
                                    validation_set = randomSplitTrainingRatings[i];
                                }
                            }
                            train_set.cache();
                            validation_set.cache();

                            start = System.nanoTime();
                            model = ALS.trainImplicit(JavaRDD.toRDD(train_set), rank, iteration, lamda, alpha);
                            elapsedTime = System.nanoTime() - start;
                            System.out.println("Train Time = "+(double)elapsedTime/1000000000 + " seconds");

                            percentileRankings[fold] = computePercentileRanking(model, validation_set);

                            System.out.println("Current Percentile ranking = " + percentileRankings[fold]);
                            sum += percentileRankings[fold];
                        }

                        double meanPercentileRanking = sum/folds;
                        if(meanPercentileRanking < bestPercentileRanking) {
                            this.model = model;
                            bestPercentileRanking = meanPercentileRanking;
                            bestAlpha = alpha;
                            bestLambda = lamda;
                            bestNumIter = iteration;
                            bestRank = rank;
                        }
                    }
                }
            }
        }

        double percentileRanking = computePercentileRanking(this.model, test_set);
        System.out.println("Percentile Ranking on test set: "+percentileRanking);
        //System.out.println("bestPercentileRanking on validation = "+bestPercentileRanking + ", bestApha="+bestAlpha+", bestLamda="+bestLambda+", bestIteratonNum="+bestNumIter+", bestRank="+bestRank);
    }

    double computePercentileRanking(MatrixFactorizationModel model, JavaRDD<Rating> validation_set) {
        // Evaluate the model on rating data
        JavaRDD<Tuple2<Object, Object>> userProducts = validation_set.map(r -> new Tuple2<>(r.user(), r.product()));

        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
                model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD()
                        .map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating()))
        );

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> predictions_and_ratings = JavaPairRDD.fromJavaRDD(validation_set
                .map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating())))
                .join(predictions);

        JavaPairRDD<Integer, Tuple3<Integer, Double, Double>> predictions_and_ratings_perUser =
                JavaPairRDD.fromJavaRDD(predictions_and_ratings.map(p -> new Tuple2<>(p._1()._1(), new Tuple3<>(p._1()._2(), p._2()._1(), p._2()._2()))));

        Map<Integer, ArrayList<ItemRating>> predictionsMap = new HashMap<>();

        for(Tuple2<Integer, Tuple3<Integer, Double, Double>> predictions_and_ratings_perUser_iter : predictions_and_ratings_perUser.collect()) {
            if(!predictionsMap.containsKey(predictions_and_ratings_perUser_iter._1())) {
                predictionsMap.put(predictions_and_ratings_perUser_iter._1(), new ArrayList<>());
            }
            ItemRating itemRating = new ItemRating();
            itemRating.item = predictions_and_ratings_perUser_iter._2()._1();
            itemRating.rating = predictions_and_ratings_perUser_iter._2()._2();
            itemRating.prediction = predictions_and_ratings_perUser_iter._2()._3();
            predictionsMap.get(predictions_and_ratings_perUser_iter._1()).add(itemRating);
        }

        double rank;
        double sumRatings = 0.0;
        double sumRatingsRanks = 0.0;

        for (Map.Entry<Integer, ArrayList<ItemRating>> entry : predictionsMap.entrySet()) {
            int total = entry.getValue().size();
            int i = 0;

            entry.getValue().sort(comparator);

            for (ItemRating itemRating : entry.getValue()) {
                double percentileRank = 1/(double)total*i;
                sumRatingsRanks += (itemRating.rating*percentileRank);
                sumRatings += itemRating.rating;

                i++;
            }
        }

        rank = sumRatingsRanks/sumRatings;
        return rank;
    }

    Comparator<ItemRating> comparator = new Comparator<ItemRating>()
    {
        public int compare(ItemRating itemRating1,
                           ItemRating itemRating2)
        {
            if (itemRating1.prediction < itemRating2.prediction) return 1;
            if (itemRating1.prediction > itemRating2.prediction) return -1;
            return 0;
        }

    };

    double computeMse(MatrixFactorizationModel model, JavaRDD<Rating> validation_set, boolean test) {
        // Evaluate the model on rating data
        JavaRDD<Tuple2<Object, Object>> userProducts = validation_set.map(r -> new Tuple2<>(r.user(), r.product()));

        long start = System.nanoTime();
        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
                model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD()
                        .map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating()))
        );
        long elapsedTime = System.nanoTime() - start;
        System.out.println("Predict Time = "+(double)elapsedTime/1000000000 + " seconds");

        JavaRDD<Tuple2<Double, Double>> ratesAndPreds = JavaPairRDD.fromJavaRDD(
                validation_set.map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating())))
                .join(predictions).values();

        double MSE = ratesAndPreds.mapToDouble(pair -> {
            double err = pair._1() - pair._2();
            if(test)
                System.out.println("rating: "+pair._1() + " - predicted: "+pair._2());
            return err * err;
        }).mean();

        return MSE;
    }
}

# Analyze-customer-transactions

This is a system for extracting statistics and recommending products to customers of a real world ecommerce website.

- Data available at kaggle: [Retailrocket recommender system dataset](https://www.kaggle.com/retailrocket/ecommerce-dataset)
- Kafka & HDFS are used to read and store csv files
- SparkR is used to extract statistics and charts
- Java Spark is used to preprocess data and built the recommendation system
    * Collaborative Filtering: implicit ALS
    * Metric: Percentile Ranking

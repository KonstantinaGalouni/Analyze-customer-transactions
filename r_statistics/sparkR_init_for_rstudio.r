### sparkR init for Rstudio ###
### taken from the official documentation: ###
### https://spark.apache.org/docs/latest/sparkr.html#starting-up-from-rstudio ###

if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "/usr/local/spark")
}
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "2g"))

library(ggplot2)


###########################################################################
############################## EVENTS #####################################
events_schema <- structType(structField("timestamp","string"),structField("visitorid","string"),structField("event","string"),structField("itemid","string"),structField("transactionid","integer"))

#read events.csv
events <- read.df("/home/myrto/Documents/bigData/datasets/events.csv",source="csv",header="true",delimiter=",", na.strings="NA",schema = events_schema)

### get statistics for events ###
createOrReplaceTempView(events,"events_table")
cacheTable("events_table")
head(sql("SELECT count(*) FROM events_table"))
head(sql("SELECT count(distinct visitorid) FROM events_table"))
head(sql("SELECT count(distinct itemid) FROM events_table"))

events_per_user <- sql("SELECT visitorid, count(*) as counted_events from events_table GROUP BY visitorid")
createOrReplaceTempView(events_per_user,"per_user_event")
cacheTable("per_user_event")
head(sql("select avg(counted_events) from per_user_event"))
head(sql("select max(counted_events) from per_user_event"))
head(sql("select count(counted_events) from per_user_event where counted_events > 1"))
head(sql("select count(counted_events) from per_user_event where counted_events > 10"))

events_per_user_r_df <- collect(events_per_user)

uncacheTable("events_table")
uncacheTable("per_user_event")

sd(events_per_user_r_df$counted_events)
median(events_per_user_r_df$counted_events)
counted_event_su <- scale(events_per_user_r_df$counted_events)
events_per_user_r_df["standard_units"] <- counted_event_su
events_per_user_plot <- ggplot(events_per_user_r_df, aes(x=standard_units)) + geom_histogram(binwidth=.5, colour="black", fill="white") + coord_cartesian(xlim = c(-4, 4))

# events <- withColumn(events, "timestamp", from_unixtime(events$timestamp/1000))
events_r_df <- collect(events)
events$timestamp <- as.POSIXct(as.numeric(events_r_df$timestamp)/1000,origin='1970-01-01')
eventsPerDay <- data.frame("dayOfTheWeek"=weekdays(as.Date(as.character(events_r_df$timestamp,"%Y-%m-%d"))), "timestamp"=events_r_df$timestamp, "visitorid"=events_r_df$visitorid, "event" = events_r_df$event, "itemid" = events_r_df$itemid)
daysVector <- c("Δευτέρα", "Τρίτη", "Τετάρτη", "Πέμπτη", "Παρασκευή", "Σάββατο", "Κυριακή")

library(plyr)
eventsPerWeekday <- count( eventsPerDay, c("dayOfTheWeek"))
eventsPerWeekday$dayOfTheWeek <- factor(eventsPerWeekday$dayOfTheWeek, levels = daysVector)
eventsPerWeekdayPlot <- ggplot(eventsPerWeekday,aes(x=dayOfTheWeek,y=freq/100))+geom_bar(stat='identity', fill="#66CCFF")+xlab("")+ylab("Counted events (in thousands)")

###########################################################################
############################ PROPERTIES ###################################
item_prop_schema <- structType(structField("timestamp","string"),structField("itemid","string"),structField("property","string"),structField("value","string"))
item_properties <- read.df("/home/myrto/Documents/bigData/datasets/item_properties_part1.csv",source="csv",header="true",schema = item_prop_schema)
item_properties_2 <- read.df("/home/myrto/Documents/bigData/datasets/item_properties_part2.csv",source="csv",header="true",schema = item_prop_schema)

#union the 2 dataframes
item_properties = union(item_properties,item_properties_2)

#convert unix epoch timestamp to datetime string in a new dataframe
item_props_with_ts <- withColumn(item_properties, "ts", from_unixtime(item_properties$timestamp/1000))
createOrReplaceTempView(item_props_with_ts,"properties_table")
head(sql("select count(*) from properties_table"))
distinct_item_prop_values <- sql("select itemid, property, value from properties_table group by itemid, property, value")
createOrReplaceTempView(distinct_item_prop_values,"distinct_properties")
cacheTable("distinct_properties")
head(sql("select count(*) from distinct_properties"))
head(sql("select count(distinct itemid) from distinct_properties"))
head(sql("select count(distinct property) from distinct_properties"))
#head(sql("select itemid, count(distinct(property)) from distinct_properties group by itemid order by itemid"))


###########################################################################
############################ CATEGORIES ###################################
#cats_ancestors_schema <- structType(structField("categoryid","string"),structField("ancestorid","string"))

#read properties csvs
#cats_ancestors_df <- read.df("/media/myrto/Files/Documents/Master/DI UOA/BigData/project/exported_csv/ancestor_per_catid.csv",source="csv",header="true",schema = cats_ancestors_schema)


#read category_tree.csv
category_tree <- read.df("/media/myrto/Files/Documents/Master/DI UOA/BigData/project/Retailrocket_recom_sys_dataset/category_tree.csv",source="csv",header="true",inferSchema="true")
createOrReplaceTempView(category_tree,"categories_table")
#get parent categories
head(sql("SELECT count(*) FROM categories_table"))
head(sql("SELECT count(distinct categoryid) FROM categories_table"))
head(sql("SELECT count(*) FROM categories_table WHERE isNull(parentid)"))


###########################################################################
################## OTHER COMBINATIONS AND TESTS ###########################

items_in_events <- sql("SELECT distinct(itemid) FROM events_table")
createOrReplaceTempView(items_in_events,"items_table")

#get items with category from properties
items_with_category <- sql("SELECT itemid,value from properties_table WHERE property LIKE 'categoryid'")

createOrReplaceTempView(items_with_category,"item_cats_from_properties")
createOrReplaceTempView(items_in_events,"used_items_table")
items_in_events_categories <- sql("SELECT distinct(i.itemid) as itemid,p.value FROM used_items_table i, item_cats_from_properties p WHERE i.itemid=p.itemid")


#get parent items ###NOT NEEDED###
createOrReplaceTempView(items_in_events_categories,"used_items_cats")
createOrReplaceTempView(parent_categories,"parents_table")
parent_items <- sql("SELECT itemid,value FROM used_items_cats WHERE value IN(SELECT * FROM parents_table)")

createOrReplaceTempView(cats_ancestors_df, "categories_with_ancestors")

#get items and their general categories per time
used_items_categories <- sql("SELECT e.itemid, e.timestamp, c.ancestorid FROM events e, item_cats_from_properties p, categories_with_ancestors c WHERE e.itemid=p.itemid AND p.property LIKE 'categoryid' AND p.value=c.categoryid")

#get weighted property values per item per event
props_per_event <- sql("SELECT e.timestamp, e.itemid, e.visitorid, i.property, max( ( 86400000/(e.timestamp-i.timestamp)) ) as weight FROM events_table e, properties_table i WHERE e.itemid=i.itemid AND i.property NOT IN('categoryid','available') AND i.timestamp < e.timestamp GROUP BY e.timestamp, e.itemid, e.visitorid, i.property")

## new events schema that includes 'implicit ratings' ##
events_with_ratings_schema <- structType(structField("timestamp","string"),structField("visitorid","string"),structField("event","string"),structField("itemid","string"),structField("transactionid","integer"), structField("eventRating","double"))

## lambda expression on events to create the new column according to event type [map-like function called dapply]
events_with_ratings <- dapply(events, function(x) { x <- { a <- 0.0
	if (x$event == "view") {
		a = 0.3
	} else if (x$event == "addtocart") {
		a = 0.7
	} else {
		a = 1.0
	}
	cbind(x, a ) }
	}, events_with_ratings_schema)

createOrReplaceTempView(events_with_ratings, "events_with_ratings_table")
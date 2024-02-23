```py
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create my_spark using .getOrCreate as it can be problematic to create more than one session.
spark = SparkSession.builder.getOrCreate()

# Print the tables in the catalog (.catalog lists all the data inside the cluster)
print(spark.catalog.listTables())


query = "FROM flights SELECT * LIMIT 10"

# Get the first 10 rows of flights
flights10 = spark.sql(query)

# Show the results
flights10.show()

# Convert the results to a pandas DataFrame for further processing
pd_counts = flights10.toPandas()

# Convert a pd dataframe to a spark dataframe by adding it as a temp view in the context. Takes one argument, the name, e.g., "temp"
pd_counts.createOrReplaceTempView("temp")

file_path = "/usr/local/share/datasets/airports.csv"

# Read in the airports data to a Spark DataFrame. header=True tells Spark to take the first row as column names.
airports = spark.read.csv(file_path, header=True)


# Create the DataFrame flights
flights = spark.table("flights")

# Show the head
flights.show()

# Add duration_hrs column into a DataFrame
flights = flights.withColumn("duration_hrs", flights.air_time/60)


# Filter flights by passing a string. This string is everything after WHERE in an SQL statement.
long_flights1 = flights.filter("distance > 1000")

# Filter flights by passing a column of boolean values (the alternative syntax to using the WHERE string).
long_flights2 = flights.filter(flights.distance > 1000)


# Select columns using strings
selected1 = flights.select("tailnum", "origin", "dest")

# Or Select columns using column objects
temp = flights.select(flights.tailnum, flights.origin, flights.dest)

# Define first filter
filterA = flights.origin == "SEA"

# Define second filter
filterB = flights.dest == "PDX"

# Filter the data, first by filterA then by filterB
selected2 = temp.filter(filterA).filter(filterB)


# Use alias to rename a column
avg_speed = (flights.distance/(flights.air_time/60)).alias("avg_speed")

# Select columns and perform a column-wise operation
speed1 = flights.select("origin", "dest", "tailnum", avg_speed)

# Create the same table using a SQL expression
speed2 = flights.selectExpr("origin", "dest", "tailnum", "distance/(air_time/60) as avg_speed")
```

# Aggregation uses .groupBy()

```py
# Find the shortest flight from PDX in terms of distance
flights.filter(flights.origin == "PDX").groupBy().min("distance").show()

# Find the longest flight from SEA in terms of air time
flights.filter(flights.origin == "SEA").groupBy().max("air_time").show()


# Average duration of Delta flights
flights.filter(flights.carrier == "DL").filter(flights.origin == "SEA").groupBy().avg("air_time").show()

# Add column in unit hours to use in aggregation of a sum - total hours in the air
flights.withColumn("duration_hrs", flights.air_time/60).groupBy().sum("duration_hrs").show()


# Import pyspark.sql.functions as F
import pyspark.sql.functions as F

# Group by month and dest
by_month_dest = flights.groupBy("month", "dest")

# Average departure delay by month and destination
by_month_dest.avg("dep_delay").show()

# use the .agg method to pass any aggregation function from pyspark.sql.functions, e.g., Standard deviation of departure delay
by_month_dest.agg(F.stddev("dep_delay")).show()
```

# JOINS

```py
# Examine the data
print(airports.show())

# When joining the column keys must maatch, so rename the faa column to "dest"
airports = airports.withColumnRenamed("faa", "dest")

# Join the DataFrames
flights_with_airports = flights.join(airports, on="dest", how="leftouter")

# Examine the new DataFrame
print(flights_with_airports.show())
```

# map() and filter() with lambda

```py
# Print my_list in the console
my_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
print("Input list is", my_list)

# Square all numbers in my_list
squared_list_lambda = list(map(lambda x: x*x, my_list))

# Print the result of the map function
print("The squared numbers are", squared_list_lambda)

# Print my_list2 in the console
my_list2 = [10, 21, 31, 40, 51, 60, 72, 80, 93, 101]
print("Input list is:", my_list2)

# Filter numbers divisible by 10
filtered_list = list(filter(lambda x: (x%10 == 0), my_list2))

# Print the numbers divisible by 10
print("Numbers divisible by 10 are:", filtered_list)
# [10, 40, 60, 80]
```

# RDDs (Resilient Distributed Datasets)
```py
# Create an RDD from a list of words
RDD = sc.parallelize(["Spark", "is", "a", "framework", "for", "Big Data processing"])

# Print out the type of the created object
print("The type of RDD is", type(RDD))
# The file type of fileRDD is <class 'pyspark.rdd.RDD'>

# Create a fileRDD from file_path
fileRDD = sc.textFile(file_path)

# Check the type of fileRDD
print("The file type of fileRDD is", type(fileRDD))
# The file type of fileRDD is <class 'pyspark.rdd.RDD'>


# Check the number of partitions in fileRDD
print("Number of partitions in fileRDD is", fileRDD.getNumPartitions)

# Create a fileRDD_part from file_path with 5 partitions
fileRDD_part = sc.textFile(file_path, minPartitions = 5)

# Check the number of partitions in fileRDD_part
print("Number of partitions in fileRDD_part is", fileRDD_part.getNumPartitions)


# Create map() transformation to cube numbers
cubedRDD = numbRDD.map(lambda x: x*x*x)

# Collect the results. NOTE: collect() should only be used on small datasets.
# Use  saveAsTextFile() or save to S3, HDFS, etc.
numbers_all = cubedRDD.collect()

# Print the numbers from numbers_all
for numb in numbers_all:
	print(numb)


# Filter the fileRDD to select lines with Spark keyword
fileRDD_filter = fileRDD.filter(lambda line: 'Spark' in line)

# How many lines are there in fileRDD?
print("The total number of lines with the keyword Spark is", fileRDD_filter.count())

# Print the first four lines of fileRDD
for line in fileRDD_filter.take(4):
  print(line)
```

# Pair RDDs (key value RDDs)
```py

# Create PairRDD Rdd with key value pairs
Rdd = sc.parallelize([(1,2), (3,4), (3,6), (4,5)])

# Apply reduceByKey() operation on Rdd
Rdd_Reduced = Rdd.reduceByKey(lambda x, y: x+y)

# Iterate over the result and print the output
for num in Rdd_Reduced.collect(): 
  print("Key {} has {} Counts".format(num[0], num[1]))


# Sort the reduced RDD with the key by descending order
Rdd_Reduced_Sort = Rdd_Reduced.sortByKey(ascending=False)

# Iterate over the result and retrieve all the elements of the RDD
for num in Rdd_Reduced_Sort.collect():
  print("Key {} has {} Counts".format(num[0], num[1]))


# Count the unique keys
total = Rdd.countByKey()

# What is the type of total?
print("The type of total is", type(total))

# Iterate over the total and print the output
for k, v in total.items(): 
  print("key", k, "has", v, "counts")

# The type of total is <class 'collections.defaultdict'>
# key 1 has 1 counts
# key 3 has 2 counts
# key 4 has 1 counts
```

# Create a base RDD and transform it to count the frequency of words in a document
```py
# Create a baseRDD from the file path
baseRDD = sc.textFile(file_path)

# Split the lines of baseRDD into words
splitRDD = baseRDD.flatMap(lambda x: x.split())

# Count the total number of words
print("Total number of words in splitRDD:", splitRDD.count())

# Convert the words in lower case and remove stop words from the stop_words curated list
splitRDD_no_stop = splitRDD.filter(lambda x: x.lower() not in stop_words)

# Create a tuple of the word and 1 
splitRDD_no_stop_words = splitRDD_no_stop.map(lambda w: (w, 1))

# Count of the number of occurences of each word
resultRDD = splitRDD_no_stop_words.reduceByKey(lambda x, y: x + y)

# Display the first 10 words and their frequencies from the input RDD
for word in resultRDD.take(10):
	print(word)

# Swap the keys and values from the input RDD
resultRDD_swap = resultRDD.map(lambda x: (x[1], x[0]))

# Sort the keys in descending order
resultRDD_swap_sort = resultRDD_swap.sortByKey(ascending=False)

# Show the top 10 most frequent words and their frequencies from the sorted RDD
for word in resultRDD_swap_sort.take(10):
	print("{},{}". format(word[1], word[0]))
```

# Spark DataFrames (higher level API than RDDs for structured data, e.g., relational database and json)
```py
# The spark session for DataFrames does what spark context does for RDDs.

# Create an RDD from the list
rdd = sc.parallelize(sample_list)

# Create a PySpark DataFrame
names_df = spark.createDataFrame(rdd, schema=['Name', 'Age'])

# Check the type of names_df
print("The type of names_df is", type(names_df))
# The type of names_df is <class 'pyspark.sql.dataframe.DataFrame'>

# Create an DataFrame from file_path. This could also be spark.read.parquet()
people_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Check the type of people_df
print("The type of people_df is", type(people_df))

# Like RDDs, DataFrames work with transformations and actions. Remember, transformations are lazy; they are performed when an action takes place. Also, transformations create a new DataFrame with your results.

# Common transformations include select(), filter(), groupby(), orderby(), dropDuplicates(), withColumbedRenamed()
# Common actions include head(), count(), columns, and describe()

# Print the first 10 observations 
people_df.show(10)

# Count the number of rows 
print("There are {} rows in the people_df DataFrame.".format(people_df.count()))

# Count the number of columns and print their names
print("There are {} columns in the people_df DataFrame and their names are {}".format(len(people_df.columns), people_df.columns))

# Select name, sex and date of birth columns
people_df_sub = people_df.select('name', 'sex', 'date of birth')

# Print the first 10 observations from people_df_sub
people_df_sub.show(10)

# Remove duplicate entries from people_df_sub
people_df_sub_nodup = people_df_sub.dropDuplicates()

# Count the number of rows
print("There were {} rows before removing duplicates, and {} rows after removing duplicates".format(people_df_sub.count(), people_df_sub_nodup.count()))
# There were 100000 rows before removing duplicates, and 99998 rows after removing duplicates
```
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

## Aggregation uses .groupBy()

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

## JOINS

# Examine the data
print(airports.show())

# When joining the column keys must maatch, so rename the faa column to "dest"
airports = airports.withColumnRenamed("faa", "dest")

# Join the DataFrames
flights_with_airports = flights.join(airports, on="dest", how="leftouter")

# Examine the new DataFrame
print(flights_with_airports.show())
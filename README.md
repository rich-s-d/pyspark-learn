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




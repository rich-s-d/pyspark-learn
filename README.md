# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create my_spark using .getOrCreate as it can be problematic to create more than one session.
my_spark = SparkSession.builder.getOrCreate()

# Print the tables in the catalog (.catalog lists all the data inside the cluster)
print(spark.catalog.listTables())

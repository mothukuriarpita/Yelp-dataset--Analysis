filePath="/FileStore/tables/n7j7oy281508866554189/2015_summary-ebaee.csv"
flight = spark.read.option("header","true"). option("inferSchema","true").csv(filePath)
#Display the contents of the dataframe
display(flight)
#Display the first 3 rows of the dataframe
flight.printSchema()
display(flight.head(3))
#Sort the dataframe on the count field in a descending order by importing library desc
from pyspark.sql.functions import desc
display(flight.sort(desc("count")))
#Sort the dataframe on the count field in a descending order using orderBy
flight.orderBy(desc("count")).collect() 
#Display the summary statistics of each of the columns
flight.describe().show()
#Find out the maximum value of the counts field.
from pyspark.sql.functions import max
flight.select(max("count").alias("maximum value")).show()
#find the top 5 countries that have the largest number of incoming flights.from pyspark.sql.functions import desc
from pyspark.sql.functions import desc
flight.groupBy("DEST_COUNTRY_NAME").agg({"count":"sum"}).sort(desc("sum(count)")).show(5)
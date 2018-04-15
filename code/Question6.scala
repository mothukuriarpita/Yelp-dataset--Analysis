filePath=("/FileStore/tables/bjvpana41508870575606/NW_Orders_01-18e61.csv")
filePath1=("/FileStore/tables/bjvpana41508870575606/NW_Order_Details-dac32.csv")
orders=spark.read.option("header","true"). option("inferSchema","true").csv(filePath)
orderDetails=spark.read.option("header","true"). option("inferSchema","true").csv(filePath1)
display(orders)
display(orderDetails)
#Find out the count of orders placed by each customer and then return the top 5 customers with the highest count of orders.
from pyspark.sql.functions import desc
orders.groupBy("CustomerID").count().sort(desc("count")).show(5)
#Join the orders and orderDetails data on the orderID column and display the results
display(orders.join(orderDetails,"orderID"))
#Join the orders and orderDetails data on the orderID column and then group the data by ShipCountry field and find the sum of quantity purchased by each country. Then sort by the sum of quantity field and list the top 10 countries.
from pyspark.sql.functions import desc
joinedData=orders.join(orderDetails,"orderID")
display(joinedData.groupBy("ShipCountry").agg({"Qty":"sum"}).sort(desc("sum(Qty)")).take(10))

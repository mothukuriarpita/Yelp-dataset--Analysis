# Yelp-dataset--Analysis
<h3><u>Description</u></h3>
This project is related to analysis of three dataset files: Business, Review and User. Business dataset contains "business_id", "full_address", "categories" columns. Review dataset contains "review_id","user_id","business_id","stars" columns. User dataset contains "user_id","name","url" columns. Spark (scala) is used to answer couple of questions like:
<ul><li>List the business_id , full address and categories of the Top 10 highest rated
  businesses using the average ratings.</li>
  <li>Read a user name from the command line and find the average of their review rating.</li>
  <li>List the 'user id' and 'stars' of users that reviewed businesses located in Stanford.</li>
  <li>List the user_id , and name of the top 10 users who have written the most
    reviews.</li>
  <li>List the business_id, and count of each business's ratings for the businesses that
    are located in the state of TX</li>
</ul>

<h3><u>Implementation</u></h3>

This is implemented using Databricks, Spark and Scala.

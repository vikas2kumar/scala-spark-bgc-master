Q1. Retrieve the top 20 movies with a minimum of 50 votes with the ranking determined by:

(numVotes/averageNumberOfVotes) * averageRating



Q2. For these 20 movies, list the persons who are most often credited and list the

different titles of the 20 movies.



The application should

be runnable in cluster;
have documentation;
be reproducible;
include unit testing


Avoid SQL statements like this as much as possible: e.g spark.sql(“select x,y,z from table1”)
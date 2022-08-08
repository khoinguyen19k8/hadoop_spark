# About
Programs in this repo are written in either Scala or Java, and then deployed to a Hadoop cluster using Docker (except the zeppelin notebooks). The cluster consists of two main nodes and 11 workers. It was provided by Neijmegen University, the Netherlands.
# MapReduce, Hadoop, and Yarn
A Java program that performs word-count across documents stored in a Hadoop file system using the MapReduce paradigm.

# Spark RDDs

Loaded the Shakespear data as an RDD then conduct some analysis.

# Spark Dataframes, Datasets and SQL

- Loaded csv data into Dataframes.
- Loaded Nijmegen tree data and query the data using Spark SQL.
- Performed analysis using Dataframe operations.
- Visualized with Geospark-zeppelin.

# Spark streaming

Wrote a scala application to parse streaming data Runscape weapon trading transaction logs using Spark streaming. Each weapon general information was known by referencing the weapons.csv file.
I then used Spark SQL and regex to filter and query relevant insight from the data.  


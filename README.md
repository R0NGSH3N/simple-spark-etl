---
layout: post
title: Use Spark as ETL tools
tags: Spark, Java, Big Data, ETL, database
categories: common
---

  Spark could be used as ETL tools, today we are going
to walk you throught how to and explain the required Spark knowledge.

## Spark Overview

`Spark Core`: have to have it

- Fundamental Component
- Task Distribution
- Scheduling
- input/output

`Spark SQL` : ***We will use this for our ETL tools***

`Spark Streaming`: we don't need it

- Streaming analytics
- Micro Batches
- Lambda Architecture

`MLlib` : we dont' need it

## Spark Data

`RDD` - Resilient Distributed Dataset, `container` that all you to work with data object.

`DataFrame` - data table

`Dataset` - combination of `RDD` and `DataFrame`, ***this is the objects we are going to work with***

## Spark Actions

`Action` and `Transformation`

`Action` call `Transformation` to do something.

![Picture 1](https://r0ngsh3n.github.io/static/img/1212/Screenshot_spark-etl-1.png)

## Streaming How-To

Spark Streaming process:

![Picture 2](https://r0ngsh3n.github.io/static/img/1212/Screenshot_spark-etl-2.png)

## Setup Environment

I use Gradle and java, so this project will NOT have any Scala or Python or Maven, following is the `build.gradle`, it list all the depencies we need

~~~groovy
dependencies {
 implementation 'org.springframework.boot:spring-boot-starter-data-jdbc'
 implementation 'org.springframework.boot:spring-boot-starter-web'
 implementation 'org.apache.spark:spark-core_2.11:2.4.4'
 implementation 'org.apache.spark:spark-sql_2.11:2.4.4'
 runtimeOnly 'mysql:mysql-connector-java'
  testImplementation 'org.springframework.boot:spring-boot-starter-test'
}
~~~

## Extractor: Connect to MySQL database

1. Create `SparkSession`
   `SparkSession` is single point of Spark Framework entry to interact with `DataFrame` and `DataSet`, with Spark 2.0, `SparkSession` become simple to create:

   ~~~java
    SparkSession spark = SparkSession.builder()
        .appName(jobName)
        .config("some option", "some value"
        //.enableHiveSupport()
        .getOrCreate();

    //set up sparkSession runtime arguments
    spark.conf().set("spark.sql.shuffle.partitions", 6);
    spark.conf().set("spark.executor.memory", "2g");
   ~~~

    If you are running on Hive FS, then you can `enbableHiveSupport` option, otherwise, it use "in memory".

   you can use following code to print out all the runtime arguments

   ~~~java
    Map<String, String> sparkConf = spark.conf().getAll();
   ~~~

   the `spark.sql.shuffle.partitions` is the number of partition when spark shuffle the data for join and aggregation. [Here](https://data-flair.training/blogs/shuffling-and-sorting-in-hadoop) is good link to introduce the "shuffle" and "sort" in `MapReduced`.

2. Implement `DataFrameReader`

    Once you have the `SparkSession`, you can build `DataFrameReader` from it:

    ~~~java
    DataFrameReader rdr = spark.read();
    rdr.format("jdbc");
    rdr.option("numPartitions", 10);
    rdr.option("partitionColumn", "Country Code");

    //JDBC connection properties
    final Properties connectionProperties = new Properties();
    connectionProperties.put("user", MYSQL_USERNAME);
    connectionProperties.put("password", MYSQL_PWD);
    String dbTable = "(SELECT * FROM HNPQCountry) AS t";

    long start_time = System.currentTimeMillis();
    Dataset<Row> jdbcDF =
            spark.read()
                    .jdbc(MYSQL_CONNECTION_URL, dbTable, connectionProperties);

    ~~~

    Set `format` for `DataFrameReader`:

    ~~~java
    rdr.format("jdbc");
    ~~~

    `format`: DataFrame Source Data Format: json,csv (since 2.0.0),parquet (see Parquet),orc,text,jdbc, libsvm.

    `jdbc`: This will connect to database with jdbc connection, there are 3 `jdbc()` methods:

    ~~~java
    jdbc( url: String,
          table: String,
          predicates: Array[String],
          connectionProperties: Properties): DataFrame

    jdbc( url: String,
          table: String,
          properties: Properties): DataFrame

    jdbc( url: String,
          table: String,
          columnName: String,
          lowerBound: Long,
          upperBound: Long,
          numPartitions: Int,
          connectionProperties: Properties): DataFrame
    ~~~
  
3. Read through the `DataSet`:

   ~~~java
    System.out.println(jdbcDF.schema());
    jdbcDF.show();
   ~~~


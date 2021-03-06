---
layout: post
title: Use Spark as ETL tools
tags: Spark, Java, Big Data, ETL, database
categories: common
---

Spark could be used as ETL tools, today we are going
to walk you throught how to and explain the required Spark knowledge.

**Why I start this ETL project**

Many Spark ETL projects out there, but I found a unique case that I can't use others:

- Source Database/Target Database doesn't support standard JDBC
- I need to control on the JDBC connections so won't crash the databases.
- I need to do pre-validation and post-recon on data. The validation will implements aggregation and counting (that is why Spark kick in)
- Data need to be store in memory cache (hazelcast) and then persistent to MongoDB.

# Simple Spark ETL Project Structure Overview

The Simple Spark ETL project has 3 components (sub projects):

1. simple-spark-etl-spring-boot
2. simple-spark-etl-job
3. simple-spark-etl-filewatcher

# Simple-spark-etl-spring-boot

   This project provide:

   1. Restful API to start/monitor/stop etl job.
   2. contain Angular project providing GUI that integrate with Spark/MongoDB/HazelCast

## Simple-spark-etl-filewatcher

This component watch the multiple file directories that user config, when there is new csv file dropped in the folder, it will load the data and store into database. This is very common operation of ETL tool.

This project use typesafe package for configuration and use multiple threads to monitor 1 to many directories.

`SimpleSparkEtlFileWatcherApplication` is start point.

~~~java
    public static void main(String[] args) {
        SimpleSparkEtlFilWatcherConfig simpleSparkEtlJobConfig = SimpleSparkEtlFilWatcherConfig.loadConfig();
        SimpleSparkEtlProcessor processor = new SimpleSparkEtlProcessor(simpleSparkEtlJobConfig);
        processor.run();
    }
~~~

### Filewatcher Configuration

The Filewatcher config is `SimpleSparkEtlJobConfig`, it consist of 3 configurations:

~~~java
    private List<SourceConfig> sourceConfigs;
    private SparkConfig sparkConfig;
    private CacheConfig cacheConfig;
~~~

The configuration contains:

1. List of `SourceConfig` define the list of directories that file watcher will monitoring on, each `SourceConfig` contains

   - Source Configuration: source file directory, source file pattern, polling time etc
   - Destination DB configuration: JDBC url, table name, username & password etc
  
2. Spark Configuration: Spark Configuration is not **ONLY** config the spark cluster related the parameters, but it is also control which Jar to run the spark job, that is very important, because the jar contain the ETL job runner control how data get extract, transform and load into database.

3. Cache(HazelCast) Configuration

~~~java
    @Override
    public void run(String... args) {
        SimpleSparkEtlFilWatcherConfig simpleSparkEtlJobConfig = SimpleSparkEtlFilWatcherConfig.loadConfig();
        SparkConfig sparkConfig = (SparkConfig) this.applicationContext.getBean("clusterSparkConfig");
        SimpleSparkEtlProcessor processor = new SimpleSparkEtlProcessor(simpleSparkEtlJobConfig, null, sparkConfig);
        jrocessor.run();
    }
~~~

The `SimpleSparkEtlFilWatcherConfig` has all the properties that file watcher needed. the properties are loaded from `application.conf` from `resources` folder

This Filewatcher is running on Spring framework and monitoring the file folder, once there are new csv file arrived, then:

1. start Spark job and submit the csv file to the spark cluster.
2. if the spark job completed, move the data file to the `completed` folder.
3. if the spark job failed, move the data file to the `error` folder.
4. Notify the result to the `simple-spark-etl-spring-boot` server.
5. Trigger the recon job start between memory cache and database.

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

`Dataset` - combination of `RDD` and `DataFrame`, this is only available on driver node. 

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

and there are some sample implementation of the project

## simple-spark-etl-spring-boot

This is Restful server run on Spring, user can send trigger to start the job


## Configuration

There are 2 type of config file we need to run spark ETL tools:

1. SparkConfig: include spark home, master, service jar and jars etc information.
2. JobConfig: includ job related arguments

`SparkConfig` used by `Spark Launcher` to prepare the spark session objectd on driver node.

`JobConfig` also used by `Spark Launcher` but the purpose is to deliver job related the infomation to spark standalone application as command line arguments

Those 2 config files are required by all `SimpleSpark-ETL-Job` caller.



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

4. Implement the `aggregation`

   Once we have `DataFrame`, we can play with the data with all kind function, I use `groupBy` as example:

   ~~~java
   jdbcDF.groupBy("Country_Code").sum().show();
   ~~~
   
   This one will group by `Country_Code` column and sum on **ALL** the digital column, result like this:

   ~~~java
   +------------+--------------------+--------------------+--------------------+--------------------+--------------------+
   |Country_Code|           sum(1995)|           sum(2000)|           sum(2005)|           sum(2010)|           sum(2014)|
   +------------+--------------------+--------------------+--------------------+--------------------+--------------------+
   |         EAS|302992017838977.4...|350595619274697.8...|397589301387791.2...|550533981011558.6...|666929288737934.7...|
   |         EAP|121255466317991.8...|151990701037800.3...|189329997000360.5...|316028137120939.7...|409310032870258.5...|
   |         ECS|475614521235871.3...|511463748512174.6...|567852449498619.8...|628489374891343.7...|659453050467567.6...|
   |         ECA|27974905466106.96...|25217473449463.17...|30451233077270.98...|37193410278270.26...|40837018780017.00...|
   |         NOC|98372858176692.73...|102378340664075.2...|132196820134356.7...|174740342172317.9...|198518419756351.5...|
   |         OEC|1055033081229600....|1216236723392884....|1332894394681830....|1424787846815576....|1525648583729085....|
   |         LCN|118009341257234.7...|127470278101624.8...|148731647744058.2...|180964490134110.1...|199491577977210.4...|
   |         LAC|98856711072961.06...|105800493395128.3...|124357683124228.9...|148862492410149.8...|161834854535672.2...|
   |         LIC|10745588371020.55...|10733019421644.43...|12047456442638.36...|16306363466644.66...|20659063105024.20...|
   |         LMC|94393437168337.15...|95464415362693.38...|117748083243199.3...|156434488833833.6...|179259793136739.2...|
   ~~~
   
5. Implement `global temp view`

   Some people will prefer to use T-SQL or spark's function, that also could be done:

   ~~~java
   jdbcDF.createGlobalTempView("wealth_accounts");
   spark.sql("select Country_Code, sum(1995) from global_temp.wealth_accounts group by Country_Code").show();
   ~~~ 
   
   first, you need to create `temp view`, I think that is equivalent to temp table in db, and then you can run sql against.
   
   p.s: when I test this function, some how I have to disable the `.enableHiveSupport()` option.


## Start Job 
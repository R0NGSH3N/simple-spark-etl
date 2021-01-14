package com.r0ngsh3n.simplesparketl.controller;

import com.google.common.base.Splitter;
import com.r0ngsh3n.simplesparketl.config.JobConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map;

import java.util.List;
import java.util.Properties;

@RestController
public class StartJobRestController {
    private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/Test";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PWD = "123";

    @GetMapping("/startJob/{jobName}")
    public void startJob(@PathVariable String jobName){
        SparkSession spark = SparkSession.builder()
                .appName(jobName)
                .master("local")
                .config("some option", "some value")
//                .enableHiveSupport()
                .getOrCreate();

        //set up sparkSession runtime arguments
        spark.conf().set("spark.sql.shuffle.partitions", 6);
        spark.conf().set("spark.executor.memory", "2g");

        Map<String, String> sparkConf = spark.conf().getAll();

        DataFrameReader rdr = spark.read();
        rdr.format("jdbc");
        rdr.option("numPartitions", 10);
        rdr.option("partitionColumn", "Country Code");
//        java.util.Map<String, String> options = Splitter.on(",").withKeyValueSeparator(":").split("driver");

        //JDBC connection properties
        final Properties connectionProperties = new Properties();
        connectionProperties.put("user", MYSQL_USERNAME);
        connectionProperties.put("password", MYSQL_PWD);
        String dbTable = "(SELECT * FROM HNPQCountry) AS t";

        long start_time = System.currentTimeMillis();
        Dataset<Row> jdbcDF =
                spark.read()
                        .jdbc(MYSQL_CONNECTION_URL, dbTable, connectionProperties);

//        List<Row> dataTable = jdbcDF.collectAsList();
//
//        dataTable.forEach(System.out::println);
        System.out.println("time spent: " + (System.currentTimeMillis() - start_time));
        System.out.println(jdbcDF.schema());
        jdbcDF.show();
    }

    private JobConfig createJobConfig(){
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobName("Test");

        return jobConfig;

    }
}

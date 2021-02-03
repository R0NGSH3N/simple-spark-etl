package com.r0ngsh3n.simplesparketl.service;

import com.r0ngsh3n.simplesparketl.config.SampleJobSparkConfigCluster;
import com.r0ngsh3n.simplesparketl.job.config.SparkConfig;
import com.r0ngsh3n.simplesparketl.job.core.jobrunner.JobRunner;
import com.r0ngsh3n.simplesparketl.job.core.submitter.SparkSubmitter;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
public class SimpleSparkEtlSparkService {

    private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/Test";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PWD = "123";
    @Autowired
    private SampleJobSparkConfigCluster sparkConfig;

    @Autowired
    private JobRunner standaloneJobRunner;

    @Autowired
    private SparkConfig standaloneSparkConfig;

    @Autowired
    private SparkConfig clusterSparkConfig;

    public void runSparkInCluster() {
        SparkSubmitter sparkSubmitter = new SparkSubmitter(this.clusterSparkConfig);
        sparkSubmitter.submit();
    }

    @Async
    public void runSparkStandalone(String jobName, int partition ) throws AnalysisException {

        SparkSession spark = SparkSession.builder()
                .appName(standaloneSparkConfig.getJobName())
                .master(standaloneSparkConfig.getMaster())
//                .config("some option", "some value")
//                .enableHiveSupport()
                .getOrCreate();

        //set up sparkSession runtime arguments
        standaloneSparkConfig.getSparkSessionOptions().forEach((k, v) -> spark.conf().set(k, v));

        standaloneJobRunner.run(spark);

//        Map<String, String> sparkConf = spark.conf().getAll();
//
//        //JDBC connection properties
//        final Properties connectionProperties = new Properties();
//        connectionProperties.put("user", MYSQL_USERNAME);
//        connectionProperties.put("password", MYSQL_PWD);
//        String dbTable = "(SELECT * FROM wealth_accounts) AS t";
//
//        long start_time = System.currentTimeMillis();
//        Dataset<Row> jdbcDF =
//                spark.read()
//                        .jdbc(MYSQL_CONNECTION_URL, dbTable, connectionProperties);
//
////        List<Row> dataTable = jdbcDF.collectAsList();
////
////        dataTable.forEach(System.out::println);
//        System.out.println("time spent: " + (System.currentTimeMillis() - start_time));
////        System.out.println(jdbcDF.schema());
//        jdbcDF.groupBy("Country_Code").sum().show();
//        jdbcDF.createGlobalTempView("wealth_accounts");
//
//        spark.sql("select Country_Code, sum(1995) from global_temp.wealth_accounts group by Country_Code").show();
//        return CompletableFuture.completedFuture("successful");
    }
}

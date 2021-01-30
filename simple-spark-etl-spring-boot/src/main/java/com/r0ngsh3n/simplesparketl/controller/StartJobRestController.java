package com.r0ngsh3n.simplesparketl.controller;

import com.r0ngsh3n.simplesparketl.job.core.JobConfig;
import com.r0ngsh3n.simplesparketl.job.core.JobContext;
import com.r0ngsh3n.simplesparketl.job.core.JobRunner;
import com.r0ngsh3n.simplesparketl.job.samplejob.SampleJobEvent;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import scala.collection.immutable.Map;

import java.util.Properties;

@RestController
@Setter
@Getter
public class StartJobRestController {
    private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/Test";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PWD = "123";

//    @Autowired
//    private java.util.Map<String, JobRunner> jobMapping;
//
//    @GetMapping("/selectJob/{jobName}")
//    public String selectJob(@PathVariable String jobName) {
//        JobRunner jobRunner = jobMapping.get(jobName);
//        SampleJobEvent event = new SampleJobEvent();
//        JobContext<SampleJobEvent> jobContext = new JobContext<>();
//        jobContext.setTarget(event);
//        if (jobRunner != null) {
//            jobRunner.run(jobContext);
//        }
//
//        return "Successful";
//    }

    @GetMapping("/startJob/{jobName}/{partition}")
    public void startJob(@PathVariable String jobName, @PathVariable int partition) throws AnalysisException {
        SparkSession spark = SparkSession.builder()
                .appName(jobName)
                .master("local")
//                .config("some option", "some value")
//                .enableHiveSupport()
                .getOrCreate();

        //set up sparkSession runtime arguments
        spark.conf().set("spark.sql.shuffle.partitions", partition);
        spark.conf().set("spark.executor.memory", "2g");

        Map<String, String> sparkConf = spark.conf().getAll();


        //JDBC connection properties
        final Properties connectionProperties = new Properties();
        connectionProperties.put("user", MYSQL_USERNAME);
        connectionProperties.put("password", MYSQL_PWD);
        String dbTable = "(SELECT * FROM wealth_accounts) AS t";

        long start_time = System.currentTimeMillis();
        Dataset<Row> jdbcDF =
                spark.read()
                        .jdbc(MYSQL_CONNECTION_URL, dbTable, connectionProperties);

//        List<Row> dataTable = jdbcDF.collectAsList();
//
//        dataTable.forEach(System.out::println);
        System.out.println("time spent: " + (System.currentTimeMillis() - start_time));
//        System.out.println(jdbcDF.schema());
        jdbcDF.groupBy("Country_Code").sum().show();
        jdbcDF.createGlobalTempView("wealth_accounts");

        spark.sql("select Country_Code, sum(1995) from global_temp.wealth_accounts group by Country_Code").show();
        SparkSubmitter sparkSubmitter = new

    }
}

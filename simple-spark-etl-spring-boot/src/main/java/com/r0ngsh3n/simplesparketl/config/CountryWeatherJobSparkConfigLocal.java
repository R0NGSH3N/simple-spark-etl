package com.r0ngsh3n.simplesparketl.config;

import com.r0ngsh3n.etl.cw.CountryWeatherJobEvent;
import com.r0ngsh3n.etl.cw.CountryWeatherLoader;
import com.r0ngsh3n.simplesparketl.job.config.JobConfig;
import com.r0ngsh3n.simplesparketl.job.config.SparkConfig;
import com.r0ngsh3n.simplesparketl.job.core.JobContext;
import com.r0ngsh3n.simplesparketl.job.core.extractor.CSVDataExtractor;
import com.r0ngsh3n.simplesparketl.job.core.extractor.Extractor;
import com.r0ngsh3n.simplesparketl.job.core.jobrunner.JobRunner;
import com.r0ngsh3n.simplesparketl.job.core.loader.Loader;
import com.r0ngsh3n.simplesparketl.job.core.transformer.AbstractDataTransformer;
import com.r0ngsh3n.simplesparketl.job.core.transformer.Transformer;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.util.HashMap;
import java.util.Map;

@Configuration
@PropertySource("classpath:country-weather-job-standalone.yml")
@ConfigurationProperties
@Getter
@Setter
public class CountryWeatherJobSparkConfigLocal {

    private String jobName;
    private String master;
    private String partition;
    private String dbConnectionURL;
    private String userName;
    private String password;
    private String dbTable;

    @Bean(name = "CountryWeatherJobConfig")
    public JobConfig CountryWeatherJobConfig() {
        JobConfig CountryWeatherJobConfig = new JobConfig();
        CountryWeatherJobConfig.setDbConnectionURL(this.dbConnectionURL);
        CountryWeatherJobConfig.setUserName(this.userName);
        CountryWeatherJobConfig.setPassword(this.password);
        CountryWeatherJobConfig.setDbTable(this.dbTable);
        CountryWeatherJobConfig.setInputCSVFileDir("/home/rongshen/Downloads/test.csv");

        return CountryWeatherJobConfig;
    }

    @Bean(name = "CountryWeatherSparkConfig")
    public SparkConfig CountryWeatherSparkConfig() {
        SparkConfig sparkConfig = new SparkConfig();
        sparkConfig.setJobName(this.jobName);
        sparkConfig.setMaster(this.master);

        Map<String, String> sessionConfigs = new HashMap<>();
        sessionConfigs.put("spark.sql.shuffle.partitions", partition);
        sessionConfigs.put("spark.executor.memory", "2g");
        sparkConfig.setSparkSessionOptions(sessionConfigs);
        return sparkConfig;
    }

    @Bean(name = "spark")
    public SparkSession sparkSession(@Qualifier("CountryWeatherSparkConfig") SparkConfig CountryWeatherSparkConfig) {
        SparkSession spark = SparkSession.builder()
                .appName(CountryWeatherSparkConfig.getJobName())
                .master(CountryWeatherSparkConfig.getMaster())
//                .config("some option", "some value")
//                .enableHiveSupport()
                .getOrCreate();

        return spark;
    }

    @Bean(name = "CountryWeatherJobRunner")
    public JobRunner<CountryWeatherJobEvent> CountryWeatherJobRunner(JobConfig sampleJobConfig, SparkSession spark) {
        Extractor<CountryWeatherJobEvent> CWExtractor = new CSVDataExtractor<CountryWeatherJobEvent>() {
            @Override
            public void postProcess(JobContext<CountryWeatherJobEvent> jobContext) {
                //for nothing
            }
        };

        Transformer<CountryWeatherJobEvent> CWTransformer = new AbstractDataTransformer<CountryWeatherJobEvent>() {
            @Override
            public void process(JobContext<CountryWeatherJobEvent> jobContext, SparkSession spark) {
                CountryWeatherJobEvent jobEvent = jobContext.getTarget();
                Dataset<Row> dataset = jobContext.getDataSet();
            }
        };
        Loader<CountryWeatherJobEvent> CWLoader = new CountryWeatherLoader();
        JobRunner<CountryWeatherJobEvent> jobRunner = new JobRunner(sampleJobConfig, CWExtractor, CWTransformer, CWLoader, spark);
        return jobRunner;
    }
}

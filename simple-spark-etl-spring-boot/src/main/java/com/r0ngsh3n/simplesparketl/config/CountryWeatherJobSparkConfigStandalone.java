package com.r0ngsh3n.simplesparketl.config;

import com.r0ngsh3n.etl.cw.CountryWeatherJobEvent;
import com.r0ngsh3n.etl.cw.CountryWeatherLoader;
import com.r0ngsh3n.etl.cw.CountryWeatherTranformer;
import com.r0ngsh3n.simplesparketl.job.config.JobConfig;
import com.r0ngsh3n.simplesparketl.job.config.SparkConfig;
import com.r0ngsh3n.simplesparketl.job.core.extractor.DBDataExtractor;
import com.r0ngsh3n.simplesparketl.job.core.extractor.Extractor;
import com.r0ngsh3n.simplesparketl.job.core.jobrunner.JobRunner;
import com.r0ngsh3n.simplesparketl.job.core.loader.Loader;
import com.r0ngsh3n.simplesparketl.job.core.transformer.Transformer;
import lombok.Getter;
import lombok.Setter;
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
public class CountryWeatherJobSparkConfigStandalone {

    private String jobName;
    private String master;
    private String partition;
    private String dbConnectionURL;
    private String userName;
    private String password;
    private String dbTable;

    @Bean(name = "CountryWeatherJobConfig")
    public JobConfig CountryWeatherJobConfig(){
        JobConfig CountryWeatherJobConfig= new JobConfig();
        CountryWeatherJobConfig.setDbConnectionURL(this.dbConnectionURL);
        CountryWeatherJobConfig.setUserName(this.userName);
        CountryWeatherJobConfig.setPassword(this.password);
        CountryWeatherJobConfig.setDbTable(this.dbTable);

        return CountryWeatherJobConfig;
    }

    @Bean(name = "CountryWeatherSparkConfig")
    public SparkConfig CountryWeatherSparkConfig(){
        SparkConfig sparkConfig = new SparkConfig();
        sparkConfig.setJobName(this.jobName);
        sparkConfig.setMaster(this.master);

        Map<String, String> sessionConfigs = new HashMap<>();
        sessionConfigs.put("spark.sql.shuffle.partitions", partition);
        sessionConfigs.put("spark.executor.memory", "2g");
        sparkConfig.setSparkSessionOptions(sessionConfigs);
        return sparkConfig;

    }

    @Bean(name = "CountryWeatherJobRunner")
    public JobRunner<CountryWeatherJobEvent> CountryWeatherJobRunner(JobConfig sampleJobConfig ){
        Extractor<CountryWeatherJobEvent> CWExtractor = new DBDataExtractor();
        Transformer<CountryWeatherJobEvent> CWTransformer = new CountryWeatherTranformer();
        Loader<CountryWeatherJobEvent> CWLoader = new CountryWeatherLoader();
        JobRunner<CountryWeatherJobEvent> jobRunner = new JobRunner(sampleJobConfig, CWExtractor, CWTransformer,  CWLoader);
        return jobRunner;
    }
}

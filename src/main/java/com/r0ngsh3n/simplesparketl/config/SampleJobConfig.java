package com.r0ngsh3n.simplesparketl.config;

import com.r0ngsh3n.simplesparketl.core.JobConfig;
import com.r0ngsh3n.simplesparketl.core.JobContext;
import com.r0ngsh3n.simplesparketl.core.JobRunner;
import com.r0ngsh3n.simplesparketl.core.loader.DBDataLoader;
import com.r0ngsh3n.simplesparketl.core.loader.Loader;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.util.Map;

@Configuration
@PropertySource("classpath:samplejob.yml")
@ConfigurationProperties
@Getter
@Setter
public class SampleJobConfig {

    private String jobName;
    private Boolean enableHiveSupport;
    private String dbConnectionURL;
    private String dbTable;
    private String sourceFormat;
    private String userName;
    private String password;

    public Map<String, String> sessionConfig;
    public Map<String, String> dataFrameOptions;

    @Bean
    public JobConfig sampleJobConfig(){
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobName(this.jobName);
        jobConfig.setSourceFormat(this.sourceFormat);
        jobConfig.setDbConnectionURL(this.dbConnectionURL);
        jobConfig.setDbTable(this.dbTable);
        jobConfig.setUserName(this.userName);
        jobConfig.setPassword(this.password);
        jobConfig.setSparkSessionOptions(sessionConfig);
        jobConfig.setDataFrameOptions(this.dataFrameOptions);
        return jobConfig;
    }

    @Bean
    public JobContext sampleJobContext(JobConfig sampleConfig){
        JobContext jobContext = new JobContext();
        jobContext.setJobConfig(sampleConfig);
        return jobContext;
    }

    @Bean
    public JobRunner SampleJobRunner(JobContext jobContext){
        JobRunner jobRunner = new JobRunner(jobContext);
        Loader dbDataLoader = new DBDataLoader();
        return jobRunner;
    }
}

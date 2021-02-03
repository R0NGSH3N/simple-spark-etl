package com.r0ngsh3n.simplesparketl.config;

import com.r0ngsh3n.simplesparketl.job.config.JobConfig;
import com.r0ngsh3n.simplesparketl.job.config.SparkConfig;
import com.r0ngsh3n.simplesparketl.job.core.extractor.DBDataExtractor;
import com.r0ngsh3n.simplesparketl.job.core.extractor.Extractor;
import com.r0ngsh3n.simplesparketl.job.core.jobrunner.JobRunner;
import com.r0ngsh3n.simplesparketl.job.core.loader.Loader;
import com.r0ngsh3n.simplesparketl.job.core.transformer.Transformer;
import com.r0ngsh3n.simplesparketl.job.samplejob.SampleJobEvent;
import com.r0ngsh3n.simplesparketl.job.samplejob.SampleLoader;
import com.r0ngsh3n.simplesparketl.job.samplejob.SampleTranformer;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.util.HashMap;
import java.util.Map;

@Configuration
@PropertySource("classpath:spark-config-standalone.yml")
@ConfigurationProperties
@Getter
@Setter
public class SampleJobSparkConfigStandalone {

    private String jobName;
    private String master;
    private String partition;
    private String dbConnectionURL;
    private String userName;
    private String password;
    private String dbTable;

    @Bean(name = "SampleJobConfig")
    public JobConfig sampleJobConfig(){
        JobConfig sampleJobConfig = new JobConfig();
        sampleJobConfig.setDbConnectionURL(this.dbConnectionURL);
        sampleJobConfig.setUserName(this.userName);
        sampleJobConfig.setPassword(this.password);
        sampleJobConfig.setDbTable(this.dbTable);

        return sampleJobConfig;
    }

    @Bean(name = "standaloneSparkConfig")
    public SparkConfig standaloneSparkConfig(){
        SparkConfig sparkConfig = new SparkConfig();
        sparkConfig.setJobName(this.jobName);
        sparkConfig.setMaster(this.master);

        Map<String, String> sessionConfigs = new HashMap<>();
        sessionConfigs.put("spark.sql.shuffle.partitions", partition);
        sessionConfigs.put("spark.executor.memory", "2g");
        return sparkConfig;

    }

    @Bean(name = "standaloneJobRunner")
    public JobRunner standalonJobRunner(JobConfig sampleJobConfig ){
        Extractor<SampleJobEvent> sampleExtractor = new DBDataExtractor();
        sampleExtractor.setJobConfig(sampleJobConfig);
        Transformer<SampleJobEvent> sampleTransformer = new SampleTranformer<>();
        Loader<SampleJobEvent> sampleLoader = new SampleLoader<>();
        JobRunner jobRunner = new JobRunner(sampleJobConfig, sampleExtractor, sampleTransformer, sampleLoader);
        return jobRunner;
    }
}

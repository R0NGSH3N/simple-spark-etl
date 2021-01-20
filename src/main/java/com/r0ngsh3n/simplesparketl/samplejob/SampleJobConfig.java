package com.r0ngsh3n.simplesparketl.samplejob;

import com.google.common.base.Splitter;
import com.r0ngsh3n.simplesparketl.core.JobConfig;
import com.r0ngsh3n.simplesparketl.core.JobContext;
import com.r0ngsh3n.simplesparketl.core.JobRunner;
import com.r0ngsh3n.simplesparketl.core.extractor.Extractor;
import com.r0ngsh3n.simplesparketl.core.loader.DBDataLoader;
import com.r0ngsh3n.simplesparketl.core.loader.Loader;
import com.r0ngsh3n.simplesparketl.core.transformer.Transformer;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

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

    public String sessionConfig;

    public String outputDir;

    @Bean
    public JobConfig sampleJob(){
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobName(this.jobName);
        jobConfig.setSourceFormat(this.sourceFormat);
        jobConfig.setDbConnectionURL(this.dbConnectionURL);
        jobConfig.setDbTable(this.dbTable);
        jobConfig.setUserName(this.userName);
        jobConfig.setPassword(this.password);
        jobConfig.setSparkSessionOptions(Splitter.on(",").withKeyValueSeparator(":").split(sessionConfig));
        return jobConfig;
    }

    @Bean(name="SampleJobRunner")
    public JobRunner SampleJobRunner(JobConfig jobConfig, Extractor<SampleJobEvent> sampleExtractor){
        JobRunner jobRunner = new JobRunner(jobConfig);
        Loader dbDataLoader = new DBDataLoader();
        Transformer<SampleJobEvent> transformer = new SampleTranformer();
        jobRunner.setLoader(dbDataLoader);
        jobRunner.setExtractor(sampleExtractor);
        jobRunner.setTransformer(transformer);

        return jobRunner;
    }

    @Bean(name="sampleExtractor")
    public Extractor<SampleJobEvent> sampleExtractor(){
        SampleExtractor extractor = new SampleExtractor();
        extractor.setOutputDir(this.outputDir);
        return extractor;
    }

}

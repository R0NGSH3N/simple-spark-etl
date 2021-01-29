package com.r0ngsh3n.simplesparketl.job.samplejob;

import com.google.common.base.Splitter;
import com.google.inject.AbstractModule;
import com.r0ngsh3n.simplesparketl.job.core.JobConfig;
import com.r0ngsh3n.simplesparketl.job.core.JobRunner;
import com.r0ngsh3n.simplesparketl.job.core.extractor.CSVDataExtractor;
import com.r0ngsh3n.simplesparketl.job.core.extractor.DBDataExtractor;
import com.r0ngsh3n.simplesparketl.job.core.extractor.Extractor;
import com.r0ngsh3n.simplesparketl.job.core.loader.Loader;
import com.r0ngsh3n.simplesparketl.job.core.transformer.Transformer;
import lombok.Getter;
import lombok.Setter;
import org.springframework.context.annotation.Bean;

@Getter
@Setter
public class SampleJobConfig extends AbstractModule{

    private String jobName;
    private Boolean enableHiveSupport;
    private String dbConnectionURL;
    private String dbTable;
    private String sourceFormat;
    private String userName;
    private String password;

    public String sessionConfig;

    public String outputDir;
    public String inputCSVFileDir;

    public JobConfig sampleJob(){
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobName(this.jobName);
        jobConfig.setSourceFormat(this.sourceFormat);
        jobConfig.setDbConnectionURL(this.dbConnectionURL);
        jobConfig.setDbTable(this.dbTable);
        jobConfig.setUserName(this.userName);
        jobConfig.setPassword(this.password);
        jobConfig.setSparkSessionOptions(Splitter.on(",").withKeyValueSeparator(":").split(sessionConfig));
        jobConfig.setInputCSVFileDir(this.inputCSVFileDir);
        return jobConfig;
    }

    @Bean(name="CSVJobRunner")
    public JobRunner CSVJobRunner(JobConfig jobConfig, Loader<SampleJobEvent> sampleLoader){
        JobRunner jobRunner = new JobRunner(jobConfig);
        Extractor<SampleJobEvent> csvDataExtractor = new CSVDataExtractor<>();
        Transformer<SampleJobEvent> transformer = new SampleTranformer();
        jobRunner.setLoader(sampleLoader);
        jobRunner.setExtractor(csvDataExtractor);
        jobRunner.setTransformer(transformer);

        return jobRunner;

    }

    @Bean(name="MySQLExtractor")
    public Extractor<SampleJobEvent> MySQLExtractor(){
        Extractor<SampleJobEvent> dbDataExtractor = new DBDataExtractor();
        return dbDataExtractor;
    }

    @Bean(name="SampleJobRunner")
    public JobRunner SampleJobRunner(JobConfig jobConfig, Loader<SampleJobEvent> sampleLoader, Extractor<SampleJobEvent> MySQLExtractor){
        JobRunner jobRunner = new JobRunner(jobConfig);
        Transformer<SampleJobEvent> transformer = new SampleTranformer();
        jobRunner.setLoader(sampleLoader);
        jobRunner.setExtractor(MySQLExtractor);
        jobRunner.setTransformer(transformer);

        return jobRunner;
    }

    @Bean(name="sampleLoader")
    public Loader<SampleJobEvent> sampleLoader(){
        SampleLoader loader = new SampleLoader();
        loader.setOutputDir(this.outputDir);
        return loader;
    }

    @Override
    protected void configure() {
        this.bind(Extractor.class).to(DBDataExtractor.class);
    }
}

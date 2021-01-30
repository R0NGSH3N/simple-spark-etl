package com.r0ngsh3n.simplesparketl.job.module;

import com.google.common.base.Splitter;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.r0ngsh3n.simplesparketl.job.config.JobConfig;
import com.r0ngsh3n.simplesparketl.job.core.JobRunner;
import com.r0ngsh3n.simplesparketl.job.core.extractor.CSVDataExtractor;
import com.r0ngsh3n.simplesparketl.job.core.extractor.DBDataExtractor;
import com.r0ngsh3n.simplesparketl.job.core.extractor.Extractor;
import com.r0ngsh3n.simplesparketl.job.core.loader.Loader;
import com.r0ngsh3n.simplesparketl.job.core.transformer.Transformer;
import com.r0ngsh3n.simplesparketl.job.samplejob.SampleJobEvent;
import com.r0ngsh3n.simplesparketl.job.samplejob.SampleLoader;
import com.r0ngsh3n.simplesparketl.job.samplejob.SampleTranformer;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;

import java.lang.reflect.InvocationTargetException;

@Getter
@Setter
@Slf4j
public class JobRunnerConfigModule implements Module{

//    private String jobName;
//    private Boolean enableHiveSupport;
//    private String dbConnectionURL;
//    private String dbTable;
//    private String sourceFormat;
//    private String userName;
//    private String password;
//
//    public String sessionConfig;
//
//    public String outputDir;
//    public String inputCSVFileDir;
//
//    public JobConfig jobConfig(){
//        JobConfig jobConfig = new JobConfig();
//        jobConfig.setJobName(this.jobName);
//        jobConfig.setSourceFormat(this.sourceFormat);
//        jobConfig.setDbConnectionURL(this.dbConnectionURL);
//        jobConfig.setDbTable(this.dbTable);
//        jobConfig.setUserName(this.userName);
//        jobConfig.setPassword(this.password);
//        jobConfig.setSparkSessionOptions(Splitter.on(",").withKeyValueSeparator(":").split(sessionConfig));
//        jobConfig.setInputCSVFileDir(this.inputCSVFileDir);
//        return jobConfig;
//    }

    private JobConfig jobConfig;


//    @Bean(name="CSVJobRunner")
//    public JobRunner CSVJobRunner(JobConfig jobConfig, Loader<SampleJobEvent> sampleLoader){
//        JobRunner jobRunner = new JobRunner(jobConfig);
//        Extractor<SampleJobEvent> csvDataExtractor = new CSVDataExtractor<>();
//        Transformer<SampleJobEvent> transformer = new SampleTranformer();
//        jobRunner.setLoader(sampleLoader);
//        jobRunner.setExtractor(csvDataExtractor);
//        jobRunner.setTransformer(transformer);
//
//        return jobRunner;
//
//    }

//    @Bean(name="MySQLExtractor")
    public Extractor extractor(String className){
        try {
            return (Extractor)Class.forName (className).getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            log.error("{}", e);
            throw new RuntimeException(e.getMessage());
        }
    }

//    @Bean(name="SampleTransformer")
    public Transformer transformer(String className){
        try {
            return (Transformer)Class.forName (className).getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            log.error("{}", e);
            throw new RuntimeException(e.getMessage());
        }
    }

//    @Bean(name="sampleLoader")
    public Loader loader(String className){
        try {
            return (Loader)Class.forName (className).getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            log.error("{}", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void configure(Binder binder) {
        binder.bind(JobConfig.class).toInstance(this.jobConfig);
        binder.bind(Extractor.class).toInstance(extractor(jobConfig.getExtractorClassName()));
        binder.bind(Transformer.class).toInstance(transformer(jobConfig.getTransformClassName()));
        binder.bind(Loader.class).toInstance(loader(jobConfig.getLoaderClassName()));
    }
}

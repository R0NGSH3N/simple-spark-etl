package com.r0ngsh3n.simplesparketl.job.samplejob;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.r0ngsh3n.simplesparketl.job.config.JobConfig;
import com.r0ngsh3n.simplesparketl.job.config.JobRunnerConfigureModule;
import com.r0ngsh3n.simplesparketl.job.core.extractor.DefaultDBDataExtractor;
import com.r0ngsh3n.simplesparketl.job.core.extractor.Extractor;
import com.r0ngsh3n.simplesparketl.job.core.loader.Loader;
import com.r0ngsh3n.simplesparketl.job.core.transformer.Transformer;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;

@Getter
@Setter
@Slf4j
public class SampleJobRunnerConfigModule implements JobRunnerConfigureModule {

    private JobConfig jobConfig;

    public Extractor<SampleJobEvent> extractor(){
        DefaultDBDataExtractor<SampleJobEvent>  extractor = new DefaultDBDataExtractor<>();
        extractor.setJobConfig(this.jobConfig);
        return extractor;
    }

//    @Bean(name="SampleTransformer")
    public Transformer<SampleJobEvent> transformer(){
        return new SampleTranformer<SampleJobEvent>();
    }

//    @Bean(name="sampleLoader")
    public Loader<SampleJobEvent> loader(){
        return new SampleLoader<SampleJobEvent>();
    }

    @Override
    public void configure(Binder binder) {
        binder.bind(JobConfig.class).toInstance(this.jobConfig);
        binder.bind(Extractor.class).toInstance(extractor());
        binder.bind(Transformer.class).toInstance(transformer());
        binder.bind(Loader.class).toInstance(loader());
    }
}

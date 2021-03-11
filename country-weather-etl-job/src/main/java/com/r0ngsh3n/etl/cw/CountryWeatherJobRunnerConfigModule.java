package com.r0ngsh3n.etl.cw;

import com.google.inject.Binder;
import com.r0ngsh3n.simplesparketl.job.config.JobConfig;
import com.r0ngsh3n.simplesparketl.job.config.JobRunnerConfigureModule;
import com.r0ngsh3n.simplesparketl.job.core.extractor.DefaultDBDataExtractor;
import com.r0ngsh3n.simplesparketl.job.core.extractor.Extractor;
import com.r0ngsh3n.simplesparketl.job.core.loader.Loader;
import com.r0ngsh3n.simplesparketl.job.core.transformer.Transformer;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Getter
@Setter
@Slf4j
public class CountryWeatherJobRunnerConfigModule implements JobRunnerConfigureModule {

    private JobConfig jobConfig;

    public Extractor<CountryWeatherJobEvent> extractor(){
        DefaultDBDataExtractor<CountryWeatherJobEvent>  extractor = new DefaultDBDataExtractor<>();
        extractor.setJobConfig(this.jobConfig);
        return extractor;
    }

//    @Bean(name="SampleTransformer")
    public Transformer<CountryWeatherJobEvent> transformer(){
//        return new CountryWeatherTranformer();
        return null;
    }

//    @Bean(name="sampleLoader")
    public Loader<CountryWeatherJobEvent> loader(){
        return new CountryWeatherLoader();
    }

    @Override
    public void configure(Binder binder) {
        binder.bind(JobConfig.class).toInstance(this.jobConfig);
        binder.bind(Extractor.class).toInstance(extractor());
        binder.bind(Transformer.class).toInstance(transformer());
        binder.bind(Loader.class).toInstance(loader());
    }

    @Override
    public void setRunningParameters(Map<String, String> runningParameters) {

    }
}

package com.r0ngsh3n.simplesparketl.job;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class SimpleSparkEtlJobApplication {

    private SimpleSparkEtlJobApplication() {
        //disable construct
    }

    public static void main(String[] args){
        //read json by gson and construct object

        //use Guice.createInjector to create injector

        //call jobRunner.run to start the process
        Injector injector = Guice.createInjector(
                new JobConfig()
        )
//        @Bean(name="CSVJobRunner")
//        public JobRunner CSVJobRunner(JobConfig jobConfig, Loader<SampleJobEvent> sampleLoader){
//            JobRunner jobRunner = new JobRunner(jobConfig);
//            Extractor<SampleJobEvent> csvDataExtractor = new CSVDataExtractor<>();
//            Transformer<SampleJobEvent> transformer = new SampleTranformer();
//            jobRunner.setLoader(sampleLoader);
//            jobRunner.setExtractor(csvDataExtractor);
//            jobRunner.setTransformer(transformer);
//
//            return jobRunner;
//
//        }
    }


}

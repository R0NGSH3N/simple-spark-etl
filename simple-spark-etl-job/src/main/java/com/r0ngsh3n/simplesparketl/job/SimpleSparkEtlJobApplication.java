package com.r0ngsh3n.simplesparketl.job;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.r0ngsh3n.simplesparketl.job.core.JobRunner;
import com.r0ngsh3n.simplesparketl.job.samplejob.SampleJobConfig;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class SimpleSparkEtlJobApplication {
    private SimpleSparkEtlJobApplication() {
        //disable construct
    }

    public static void main(String[] args) {
        //read json by gson and construct object

        //use Guice.createInjector to create injector

        //call jobRunner.run to start the process
        Injector injector = Guice.createInjector(new SampleJobConfig());
        JobRunner jobRunner = injector.getInstance(JobRunner.class);
        jobRunner.run();
    }

}

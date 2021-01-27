package com.r0ngsh3n.simplesparketl.job.core;

import com.google.inject.Inject;
import com.r0ngsh3n.simplesparketl.job.core.extractor.Extractor;
import com.r0ngsh3n.simplesparketl.job.core.loader.Loader;
import com.r0ngsh3n.simplesparketl.job.core.transformer.Transformer;
import lombok.Getter;
import lombok.Setter;

@Getter
public class JobRunner<T> {

    private JobConfig jobConfig;
    private Extractor<T> extractor;
    private Loader<T> loader;
    private Transformer<T> transformer;

    @Inject
    public void setExtractor(Extractor extractor){
        this.extractor = extractor;
    }
    public JobRunner(JobConfig jobConfig){
        this.jobConfig = jobConfig;
    }

    public void run(T target){
        JobContext<T> jobContext = new JobContext<>();
        jobContext.setTarget(target);
        this.extractor.extract(jobContext);
        this.transformer.tranform(jobContext);
        this.loader.load(jobContext);
    }

    public static void main(String args[]){

    }

}

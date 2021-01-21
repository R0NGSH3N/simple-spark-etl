package com.r0ngsh3n.simplesparketl.core;

import com.r0ngsh3n.simplesparketl.core.extractor.Extractor;
import com.r0ngsh3n.simplesparketl.core.loader.Loader;
import com.r0ngsh3n.simplesparketl.core.transformer.Transformer;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class JobRunner<T> {

    private JobConfig jobConfig;
    private Extractor<T> extractor;
    private Loader<T> loader;
    private Transformer<T> transformer;

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

}

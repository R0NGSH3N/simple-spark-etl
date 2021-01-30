package com.r0ngsh3n.simplesparketl.job.core;

import com.google.inject.Inject;
import com.r0ngsh3n.simplesparketl.job.config.JobConfig;
import com.r0ngsh3n.simplesparketl.job.core.extractor.Extractor;
import com.r0ngsh3n.simplesparketl.job.core.loader.Loader;
import com.r0ngsh3n.simplesparketl.job.core.transformer.Transformer;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class JobRunner

    private JobConfig jobConfig;
    private Extractor extractor;
    private Loader loader;
    private Transformer transformer;

    @Inject
    public JobRunner(
            JobConfig jobConfig,
            Extractor extractor,
            Transformer transformer,
            Loader loader
    ) {
        this.jobConfig = jobConfig;
        this.extractor = extractor;
        this.transformer = transformer;
        this.loader = loader;
    }


    public void run(){
        JobContext jobContext = new JobContext();
        this.extractor.extract(jobContext);
        this.transformer.tranform(jobContext);
        this.loader.load(jobContext);
    }

}

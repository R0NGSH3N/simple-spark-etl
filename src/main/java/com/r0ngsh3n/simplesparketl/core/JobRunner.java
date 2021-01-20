package com.r0ngsh3n.simplesparketl.core;

import com.r0ngsh3n.simplesparketl.core.extractor.Extractor;
import com.r0ngsh3n.simplesparketl.core.loader.Loader;
import com.r0ngsh3n.simplesparketl.core.transformer.Transformer;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class JobRunner<T> {

    private JobContext jobContext;
    private Loader loader;
    private Extractor<T> extractor;
    private Transformer<T> transformer;

    public JobRunner(JobContext jobContext){
        this.jobContext = jobContext;
    }

    public void run(){
        this.loader.load(jobContext);
    }



}

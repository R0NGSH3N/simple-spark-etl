package com.r0ngsh3n.simplesparketl.job.core.extractor;

import com.r0ngsh3n.simplesparketl.job.core.JobContext;

public class DefaultDBDataExtractor<T> extends DBDataExtractor<T>{

    public void postProcess(JobContext<T> jobContext) {
        //do nothing
    }
}

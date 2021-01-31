package com.r0ngsh3n.simplesparketl.job.core.transformer;

import com.r0ngsh3n.simplesparketl.job.core.JobContext;

public interface Transformer<T>{
    public void tranform(JobContext<T> jobContect);
}

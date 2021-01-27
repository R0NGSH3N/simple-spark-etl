package com.r0ngsh3n.simplesparketl.core.transformer;

import com.r0ngsh3n.simplesparketl.core.JobContext;

public interface Transformer<T>{
    public void tranform(JobContext<T> jobContect);
}

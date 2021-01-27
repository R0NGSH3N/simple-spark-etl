package com.r0ngsh3n.simplesparketl.core.extractor;

import com.r0ngsh3n.simplesparketl.core.JobConfig;
import com.r0ngsh3n.simplesparketl.core.JobContext;

public interface Extractor<T> {
    void extract(JobContext<T> jobContext);
    void setJobConfig(JobConfig jobConfig);
}

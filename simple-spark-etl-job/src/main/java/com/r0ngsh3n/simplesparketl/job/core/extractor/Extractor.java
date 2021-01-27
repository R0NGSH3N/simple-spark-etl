package com.r0ngsh3n.simplesparketl.job.core.extractor;

import com.r0ngsh3n.simplesparketl.job.core.JobConfig;
import com.r0ngsh3n.simplesparketl.job.core.JobContext;

public interface Extractor<T> {
    void extract(JobContext<T> jobContext);
    void setJobConfig(JobConfig jobConfig);
}

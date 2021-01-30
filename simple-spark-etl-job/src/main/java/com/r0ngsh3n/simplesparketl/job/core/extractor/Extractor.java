package com.r0ngsh3n.simplesparketl.job.core.extractor;

import com.r0ngsh3n.simplesparketl.job.config.JobConfig;
import com.r0ngsh3n.simplesparketl.job.core.JobContext;

public interface Extractor {
    void extract(JobContext jobContext);
    void setJobConfig(JobConfig jobConfig);
}

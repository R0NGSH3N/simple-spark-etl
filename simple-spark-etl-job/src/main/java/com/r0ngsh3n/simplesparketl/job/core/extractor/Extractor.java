package com.r0ngsh3n.simplesparketl.job.core.extractor;

import com.r0ngsh3n.simplesparketl.job.config.JobConfig;
import com.r0ngsh3n.simplesparketl.job.core.JobContext;
import org.apache.spark.sql.SparkSession;

public interface Extractor<T> {
    void extract(JobContext<T> jobContext, SparkSession spark);
    void setJobConfig(JobConfig jobConfig);
}

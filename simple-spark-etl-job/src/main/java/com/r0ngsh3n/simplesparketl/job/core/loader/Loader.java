package com.r0ngsh3n.simplesparketl.job.core.loader;

import com.r0ngsh3n.simplesparketl.job.config.JobConfig;
import com.r0ngsh3n.simplesparketl.job.core.JobContext;
import org.apache.spark.sql.SparkSession;

public interface Loader<T> {

    public void load(JobContext<T> jobContext, SparkSession spark);
    public void setJobConfig(JobConfig jobConfig);
}

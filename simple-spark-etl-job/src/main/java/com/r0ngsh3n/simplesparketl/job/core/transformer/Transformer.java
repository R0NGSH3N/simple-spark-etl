package com.r0ngsh3n.simplesparketl.job.core.transformer;

import com.r0ngsh3n.simplesparketl.job.core.JobContext;
import org.apache.spark.sql.SparkSession;

public interface Transformer<T>{
    public void tranform(JobContext<T> jobContect, SparkSession spark);
}

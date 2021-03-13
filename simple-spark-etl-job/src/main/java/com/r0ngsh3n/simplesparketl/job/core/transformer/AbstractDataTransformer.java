package com.r0ngsh3n.simplesparketl.job.core.transformer;

import com.r0ngsh3n.simplesparketl.job.config.JobConfig;
import com.r0ngsh3n.simplesparketl.job.core.JobContext;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.SparkSession;

@Setter
@Getter
public abstract class AbstractDataTransformer<T> implements Transformer<T>{
    private JobConfig jobConfig;
    @Override
     public void tranform(JobContext<T> jobContext, SparkSession spark) {
        process(jobContext, spark);

    }

    public abstract void process(JobContext<T> jobContext, SparkSession spark);

}

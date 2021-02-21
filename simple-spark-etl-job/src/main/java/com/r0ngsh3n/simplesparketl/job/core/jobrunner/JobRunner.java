package com.r0ngsh3n.simplesparketl.job.core.jobrunner;

import com.google.inject.Inject;
import com.r0ngsh3n.simplesparketl.job.config.JobConfig;
import com.r0ngsh3n.simplesparketl.job.core.JobContext;
import com.r0ngsh3n.simplesparketl.job.core.extractor.Extractor;
import com.r0ngsh3n.simplesparketl.job.core.loader.Loader;
import com.r0ngsh3n.simplesparketl.job.core.transformer.Transformer;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.SparkSession;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class JobRunner<T> {

    private JobConfig jobConfig;
    private Extractor<T> extractor;
    private Loader<T> loader;
    private Transformer<T> transformer;
    private SparkSession spark;

    @Inject
    public JobRunner(
            JobConfig jobConfig,
            Extractor<T> extractor,
            Transformer<T> transformer,
            Loader<T> loader,
            SparkSession spark
    ) {
        this.jobConfig = jobConfig;
        this.extractor = extractor;
        extractor.setJobConfig(jobConfig);
        this.transformer = transformer;
        transformer.setJobConfig(jobConfig);
        this.loader = loader;
        loader.setJobConfig(jobConfig);
    }

    public void run(T target){
        JobContext<T> jobContext = new JobContext();
        if(target != null){
            jobContext.setTarget(target);
        }
        this.extractor.extract(jobContext, spark);
        this.transformer.tranform(jobContext, spark);
        this.loader.load(jobContext, spark);

    }

}

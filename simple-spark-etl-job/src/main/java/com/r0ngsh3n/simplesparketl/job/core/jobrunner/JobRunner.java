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
public class JobRunner {

    private JobConfig jobConfig;
    private Extractor extractor;
    private Loader loader;
    private Transformer transformer;

    @Inject
    public JobRunner(
            JobConfig jobConfig,
            Extractor extractor,
            Transformer transformer,
            Loader loader
    ) {
        this.jobConfig = jobConfig;
        this.extractor = extractor;
        extractor.setJobConfig(jobConfig);
        this.transformer = transformer;
        transformer.setJobConfig(jobConfig);
        this.loader = loader;
        loader.setJobConfig(jobConfig);
    }

    public void run(SparkSession spark){
        JobContext jobContext = new JobContext();
        this.extractor.extract(jobContext, spark);
        this.transformer.tranform(jobContext, spark);
        this.loader.load(jobContext, spark);

    }

}

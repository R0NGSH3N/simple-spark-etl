package com.r0ngsh3n.simplesparketl.job.core.extractor;

import com.r0ngsh3n.simplesparketl.job.config.JobConfig;
import com.r0ngsh3n.simplesparketl.job.core.JobContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class CSVDataExtractor<T> extends AbstractExtractor<T>{

    @Override
    public void extract(JobContext<T> jobContext, SparkSession spark) {
        JobConfig jobConfig = getJobConfig();
        Dataset<Row> csvDF =spark
                .read()
                .option("inferSchema", "true")
                .option("header", true)
                .csv(jobConfig.getInputCSVFileDir());
        jobContext.setDataSet(csvDF);
    }
}

package com.r0ngsh3n.simplesparketl.core.extractor;

import com.r0ngsh3n.simplesparketl.core.JobConfig;
import com.r0ngsh3n.simplesparketl.core.JobContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CSVDataExtractor<T> extends AbstractExtractor{

    @Override
    public void extract(JobContext jobContext) {
        JobConfig jobConfig = getJobConfig();
        if(this.spark == null){
            initSparkSession(jobConfig);
        }
        Dataset<Row> csvDF =spark.read().option("header", true).csv(jobConfig.getInputCSVFileDir());
        jobContext.setDataSet(csvDF);
    }
}

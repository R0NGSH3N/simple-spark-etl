package com.r0ngsh3n.simplesparketl.job.samplejob;

import com.r0ngsh3n.simplesparketl.job.core.JobContext;
import com.r0ngsh3n.simplesparketl.job.core.loader.Loader;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

@Getter
@Setter
public class SampleLoader implements Loader {
    private String outputDir;

    @Override
    public void load(JobContext jobContext) {
        Dataset<Row> dataset = jobContext.getDataSet();
        dataset.write().option("header", true).csv(outputDir + "output.csv");
    }
}

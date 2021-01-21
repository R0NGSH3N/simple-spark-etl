package com.r0ngsh3n.simplesparketl.samplejob;

import com.r0ngsh3n.simplesparketl.core.JobContext;
import com.r0ngsh3n.simplesparketl.core.loader.Loader;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

@Getter
@Setter
public class SampleLoader implements Loader<SampleJobEvent> {
    private String outputDir;

    @Override
    public void load(JobContext<SampleJobEvent> jobContext) {
        Dataset<Row> dataset = jobContext.getDataSet();
        dataset.write().option("header", true).csv(outputDir + "output.csv");
    }
}

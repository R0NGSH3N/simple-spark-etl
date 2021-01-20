package com.r0ngsh3n.simplesparketl.samplejob;

import com.r0ngsh3n.simplesparketl.core.JobContext;
import com.r0ngsh3n.simplesparketl.core.extractor.Extractor;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

@Getter
@Setter
public class SampleExtractor implements Extractor<SampleJobEvent> {
    private String outputDir;

    @Override
    public void extract(JobContext<SampleJobEvent> jobContext) {
        Dataset<Row> dataset = jobContext.getDataSet();
        dataset.write().option("header", true).csv(outputDir + "output.csv");
    }
}

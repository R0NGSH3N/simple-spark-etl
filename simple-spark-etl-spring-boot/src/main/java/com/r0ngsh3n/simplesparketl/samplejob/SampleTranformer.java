package com.r0ngsh3n.simplesparketl.samplejob;

import com.r0ngsh3n.simplesparketl.core.JobContext;
import com.r0ngsh3n.simplesparketl.core.transformer.Transformer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SampleTranformer implements Transformer<SampleJobEvent> {

    @Override
    public void tranform(JobContext<SampleJobEvent> jobContext) {
        SampleJobEvent jobEvent = jobContext.getTarget();
        Dataset<Row> dataset = jobContext.getDataSet();
        if (!jobEvent.ValidateSum(dataset)) {
            throw new RuntimeException("validation failed");
        }else{
            return;
        }

    }
}

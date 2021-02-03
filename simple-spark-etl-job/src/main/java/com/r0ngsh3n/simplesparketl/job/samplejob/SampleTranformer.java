package com.r0ngsh3n.simplesparketl.job.samplejob;

import com.r0ngsh3n.simplesparketl.job.core.JobContext;
import com.r0ngsh3n.simplesparketl.job.core.transformer.Transformer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SampleTranformer<SampleJobEvent> implements Transformer<SampleJobEvent> {

    @Override
    public void tranform(JobContext<SampleJobEvent> jobContext, SparkSession spark) {
        SampleJobEvent jobEvent = jobContext.getTarget();
        Dataset<Row> dataset = jobContext.getDataSet();
//        if (!jobEvent.ValidateSum(dataset)) {
//            throw new RuntimeException("validation failed");
//        }else{
//            return;
//        }

    }
}

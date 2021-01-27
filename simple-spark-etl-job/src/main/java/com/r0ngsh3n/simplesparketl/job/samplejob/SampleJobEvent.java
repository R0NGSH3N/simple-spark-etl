package com.r0ngsh3n.simplesparketl.job.samplejob;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SampleJobEvent {

    public boolean ValidateSum(Dataset<Row> dataset){
        if(dataset.count() > 0){
            return true;
        }else{
            return false;
        }
    }
}

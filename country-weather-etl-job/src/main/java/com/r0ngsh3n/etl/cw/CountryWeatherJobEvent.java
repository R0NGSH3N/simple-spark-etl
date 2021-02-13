package com.r0ngsh3n.etl.cw;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CountryWeatherJobEvent {

    public boolean ValidateSum(Dataset<Row> dataset){

        if(dataset.count() > 0){
            return true;
        }else{
            return false;
        }
    }
}

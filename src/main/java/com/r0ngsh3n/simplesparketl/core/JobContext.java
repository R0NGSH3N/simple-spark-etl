package com.r0ngsh3n.simplesparketl.core;

import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


@Setter
@Getter
public class JobContext<T> {

    private JobConfig jobConfig;
    private Dataset<Row> dataSet;
    private T target;

}

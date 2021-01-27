package com.r0ngsh3n.simplesparketl.job.core;

import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


@Setter
@Getter
public class JobContext<T> {
    private Dataset<Row> dataSet;
    private T target;
}

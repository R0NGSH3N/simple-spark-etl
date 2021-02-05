package com.r0ngsh3n.etl.cw;

import com.r0ngsh3n.simplesparketl.job.config.JobConfig;
import com.r0ngsh3n.simplesparketl.job.core.JobContext;
import com.r0ngsh3n.simplesparketl.job.core.loader.Loader;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Getter
@Setter
public class CountryWeatherLoader implements Loader<CountryWeatherJobEvent> {
    private String outputDir;
    private JobConfig jobConfig;

    @Override
    public void load(JobContext<CountryWeatherJobEvent> jobContext, SparkSession spark) {
        Dataset<Row> dataset = jobContext.getDataSet();
        dataset.write().option("header", true).csv(outputDir + "output.csv");
    }

}

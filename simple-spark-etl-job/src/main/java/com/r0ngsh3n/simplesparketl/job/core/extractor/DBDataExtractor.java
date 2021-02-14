package com.r0ngsh3n.simplesparketl.job.core.extractor;

import com.r0ngsh3n.simplesparketl.job.config.JobConfig;
import com.r0ngsh3n.simplesparketl.job.core.JobContext;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

@Getter
@Setter
public class DBDataExtractor<T> implements Extractor<T> {

    private JobConfig jobConfig;

    @Override
    public void extract(JobContext<T> jobContext, SparkSession spark) {
        final Properties connectionProperties = new Properties();
        connectionProperties.put("user", jobConfig.getUserName());
        connectionProperties.put("password", jobConfig.getPassword());
        String dbTable = jobConfig.getDbTable();

        Dataset<Row> jdbcDF = spark.read().jdbc(jobConfig.getDbConnectionURL(), dbTable, connectionProperties);
        jdbcDF.explain();

        jobContext.setDataSet(jdbcDF);

    }


}

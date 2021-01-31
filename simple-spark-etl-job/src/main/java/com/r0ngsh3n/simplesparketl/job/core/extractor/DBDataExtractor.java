package com.r0ngsh3n.simplesparketl.job.core.extractor;

import com.r0ngsh3n.simplesparketl.job.config.JobConfig;
import com.r0ngsh3n.simplesparketl.job.core.JobContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Properties;

public abstract class DBDataExtractor<T> extends AbstractExtractor<T> {

    @Override
    public void extract(JobContext<T> jobContext) {
        JobConfig jobConfig = getJobConfig();
        if(this.spark == null){
            initSparkSession(jobConfig);
        }
        final Properties connectionProperties = new Properties();
        connectionProperties.put("user", jobConfig.getUserName());
        connectionProperties.put("password", jobConfig.getPassword());
        String dbTable = jobConfig.getDbTable();

        Dataset<Row> jdbcDF = spark.read().jdbc(jobConfig.getDbConnectionURL(), dbTable, connectionProperties);

        jobContext.setDataSet(jdbcDF);

    }


}

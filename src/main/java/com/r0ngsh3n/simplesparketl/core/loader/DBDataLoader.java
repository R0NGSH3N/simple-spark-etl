package com.r0ngsh3n.simplesparketl.core.loader;

import com.r0ngsh3n.simplesparketl.core.JobConfig;
import com.r0ngsh3n.simplesparketl.core.JobContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Properties;

public class DBDataLoader<T> extends AbstractLoader{

    @Override
    public void load(JobContext jobContext) {
        JobConfig jobConfig = jobContext.getJobConfig();
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

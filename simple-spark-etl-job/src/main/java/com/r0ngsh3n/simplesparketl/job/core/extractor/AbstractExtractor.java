package com.r0ngsh3n.simplesparketl.job.core.extractor;

import com.r0ngsh3n.simplesparketl.job.config.JobConfig;
import com.r0ngsh3n.simplesparketl.job.core.JobContext;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public abstract class AbstractExtractor<T> implements Extractor<T>{

    protected SparkSession spark;
    private JobConfig jobConfig;

    public JobConfig getJobConfig(){
        return this.jobConfig;
    }

    public abstract void postProcess(JobContext<T> jobContext);
    public void setJobConfig(JobConfig jobConfig) {
        this.jobConfig = jobConfig;
    }

    public void initSparkSession(JobConfig jobconf) {
        if (jobconf.getEnableHiveSupport()) {
            this.spark = SparkSession.builder()
                    .appName(jobconf.getJobName())
                    .master("local")
                    .enableHiveSupport()
                    .getOrCreate();
        } else {
            this.spark = SparkSession.builder()
                    .appName(jobconf.getJobName())
                    .master("local")
                    .getOrCreate();
        }

        Map<String, String> sesstionOptions = jobconf.getSparkSessionOptions();
        for (String key : sesstionOptions.keySet()) {
            spark.conf().set(key, sesstionOptions.get(key));
        }

    }


}

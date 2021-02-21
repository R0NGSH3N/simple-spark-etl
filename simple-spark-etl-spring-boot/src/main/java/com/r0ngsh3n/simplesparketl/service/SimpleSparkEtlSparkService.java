package com.r0ngsh3n.simplesparketl.service;

import com.r0ngsh3n.etl.cw.CountryWeatherJobEvent;
import com.r0ngsh3n.simplesparketl.config.CountryWeatherJobSparkConfigStandalone;
import com.r0ngsh3n.simplesparketl.job.config.SparkConfig;
import com.r0ngsh3n.simplesparketl.job.core.jobrunner.JobRunner;
import com.r0ngsh3n.simplesparketl.job.core.submitter.SparkSubmitter;
import org.apache.spark.sql.AnalysisException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class SimpleSparkEtlSparkService {

    @Autowired
    private CountryWeatherJobSparkConfigStandalone sparkConfig;

    @Autowired
    private JobRunner<CountryWeatherJobEvent> CountryWeatherJobRunnerLocal;

    @Autowired
    private SparkConfig CountryWeatherSparkConfig;

    @Autowired
    private SparkConfig clusterSparkConfig;

    public void runSparkInCluster() throws Exception {
        SparkSubmitter sparkSubmitter = new SparkSubmitter(this.clusterSparkConfig);
        CompletableFuture<String> state = sparkSubmitter.submit();
        System.out.println(state.get());
        if (state.get().equals("end")) {
            throw new Exception("state is not end! " + state.get());
        }
    }

    public void runSparkLocal(CountryWeatherJobEvent event) throws AnalysisException {
        CountryWeatherJobRunnerLocal.run(event);
    }
}

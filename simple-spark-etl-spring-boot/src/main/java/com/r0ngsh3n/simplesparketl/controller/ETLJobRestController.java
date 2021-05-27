package com.r0ngsh3n.simplesparketl.controller;

import com.r0ngsh3n.etl.cw.CountryWeatherJobEvent;
import com.r0ngsh3n.simplesparketl.job.core.JobContext;
import com.r0ngsh3n.simplesparketl.job.core.jobrunner.JobRunner;
import com.r0ngsh3n.simplesparketl.service.SimpleSparkEtlSparkService;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Setter
@Getter
public class ETLJobRestController {
    @Autowired
    private SparkSession spark;

    @Autowired
    private SimpleSparkEtlSparkService sparkService;

    @Autowired
    private java.util.Map<String, JobRunner> jobMapping;

    @Autowired
    private JobRunner<CountryWeatherJobEvent> CountryWeatherJobRunner;

    @GetMapping("/selectJob/{jobName}")
    public String startCountryWeatherJob() {
        CountryWeatherJobEvent event = new CountryWeatherJobEvent();
        JobContext<CountryWeatherJobEvent> jobContext = new JobContext<>();
        jobContext.setTarget(event);

        return "Successful";
    }

    @GetMapping("/startJob")
    public void startJob() throws AnalysisException {
        String sparkMode = "Local";
        if (sparkMode.equals("Local")) {
            CountryWeatherJobEvent event = new CountryWeatherJobEvent();
            //TODO add some thing to event
            sparkService.runSparkLocal(event);
        } else {
            try {
                sparkService.runSparkInCluster();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void testJdbcRDD(SparkSession spark){

    }
}

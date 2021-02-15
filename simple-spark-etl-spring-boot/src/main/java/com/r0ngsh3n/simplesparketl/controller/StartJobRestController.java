package com.r0ngsh3n.simplesparketl.controller;

import com.r0ngsh3n.etl.cw.CountryWeatherJobEvent;
import com.r0ngsh3n.simplesparketl.job.core.JobContext;
import com.r0ngsh3n.simplesparketl.job.core.jobrunner.JobRunner;
import com.r0ngsh3n.simplesparketl.service.SimpleSparkEtlSparkService;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.AnalysisException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Setter
@Getter
public class StartJobRestController {

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
        String sparkMode = "Standalone";
        if (sparkMode.equals("Standalone")) {
            CountryWeatherJobEvent event = new CountryWeatherJobEvent();
            //TODO add some thing to event
            sparkService.runSparkStandalone(event);
        } else {
            try {
                sparkService.runSparkInCluster();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

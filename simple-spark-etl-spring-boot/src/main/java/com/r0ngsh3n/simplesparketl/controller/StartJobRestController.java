package com.r0ngsh3n.simplesparketl.controller;

import com.r0ngsh3n.simplesparketl.config.SparkConfig;
import com.r0ngsh3n.simplesparketl.service.SimpleSparkEtlSparkService;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import scala.collection.immutable.Map;

import java.util.Properties;

@RestController
@Setter
@Getter
public class StartJobRestController {


    @Autowired
    private SimpleSparkEtlSparkService sparkService;

//    @Autowired
//    private java.util.Map<String, JobRunner> jobMapping;
//
//    @GetMapping("/selectJob/{jobName}")
//    public String selectJob(@PathVariable String jobName) {
//        JobRunner jobRunner = jobMapping.get(jobName);
//        SampleJobEvent event = new SampleJobEvent();
//        JobContext<SampleJobEvent> jobContext = new JobContext<>();
//        jobContext.setTarget(event);
//        if (jobRunner != null) {
//            jobRunner.run(jobContext);
//        }
//
//        return "Successful";
//    }

    @GetMapping("/startJob/{jobName}/{partition}/{sparkMode")
    public void startJob(@PathVariable String jobName, @PathVariable int partition, @PathVariable String sparkMode) throws AnalysisException {
        if(sparkMode.equals("Standalone")){
            sparkService.runSparkStandalone(jobName, partition);
        }else{

        }

    }
}

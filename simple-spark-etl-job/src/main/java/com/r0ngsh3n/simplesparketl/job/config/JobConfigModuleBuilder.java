package com.r0ngsh3n.simplesparketl.job.config;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.r0ngsh3n.simplesparketl.job.module.JobRunnerConfigModule;

import java.io.*;

public final class JobConfigModuleBuilder {
   private JobConfig jobConfig;

    public JobConfigModuleBuilder setConfigFile(String configFileName){
        try {
            InputStreamReader reader = new InputStreamReader(new FileInputStream(new File(configFileName)));
            this.jobConfig = new ObjectMapper(new YAMLFactory()).readValue(reader, JobConfig.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return this;
    }


    public JobRunnerConfigModule build(){
        JobRunnerConfigModule configModule = new JobRunnerConfigModule();

        configModule.setJobConfig(this.jobConfig);
        return new JobRunnerConfigModule();
    }



}

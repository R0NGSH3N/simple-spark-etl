package com.r0ngsh3n.simplesparketl.job.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@Slf4j
public final class JobConfigModuleBuilder {
    private JobConfig jobConfig;
    private Map<String, String> runningParameters;

    public JobConfigModuleBuilder setConfigFile(String configFileName) {
        try {
            log.info(String.format("Config file name is : %s", configFileName));
            InputStreamReader reader = new InputStreamReader(new FileInputStream(new File(configFileName)));
            this.jobConfig = new ObjectMapper(new YAMLFactory()).readValue(reader, JobConfig.class);
        } catch (IOException e) {
            log.error("{}", e);
            e.printStackTrace();
        }

        return this;
    }

    public void parseRunningParams(CommandLine line){
        this.runningParameters = new HashMap<>();
        this.jobConfig.getJobArguments().keySet().forEach(job -> this.runningParameters.put(job, line.getOptionValue(job)));
    }

    public JobRunnerConfigureModule build() throws Exception {
        JobRunnerConfigureModule configModule = (JobRunnerConfigureModule) Class.forName(this.jobConfig.getJobRunnerConfigureModuleClazzName()).getConstructor().newInstance();
        configModule.setJobConfig(this.jobConfig);
        configModule.setRunningParameters(this.runningParameters);
        return configModule;
    }

    public Options getOptions(){
       jobConfig.getJobArguments().forEach((k,v) -> log.info("required arguments: " + k)) ;
       Options options = new Options();
       jobConfig.getJobArguments().forEach((k,v) -> options.addOption(new Option(k, v)));
       return options;
    }

}

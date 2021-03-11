package com.r0ngsh3n.simplesparketl.job.config;

import com.google.inject.Module;

import java.util.Map;

public interface JobRunnerConfigureModule extends Module {
    public void setJobConfig(JobConfig jobConfig);

    public void setRunningParameters(Map<String, String> runningParameters);

}

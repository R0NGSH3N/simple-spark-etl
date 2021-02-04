package com.r0ngsh3n.simplesparketl.job.config;
import com.google.inject.Module;

public interface JobRunnerConfigureModule extends Module{
    public void setJobConfig(JobConfig jobConfig);
}

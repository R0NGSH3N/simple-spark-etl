package com.r0ngsh3n.simplesparketl.core;

import com.r0ngsh3n.simplesparketl.core.loader.Loader;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class JobRunner {

    private JobContext jobContext;
    private Loader loader;

    public JobRunner(JobContext jobContext){
        this.jobContext = jobContext;
    }

    public void run(){
        this.loader.load(jobContext);
    }



}

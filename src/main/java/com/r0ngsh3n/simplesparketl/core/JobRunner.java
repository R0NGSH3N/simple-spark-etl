package com.r0ngsh3n.simplesparketl.core;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class JobRunner {

    private JobContext jobContext;

    public JobRunner(JobContext jobContext){
        this.jobContext = jobContext;
    }


}

package com.r0ngsh3n.simplesparketl.config;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Setter
@Getter
public class JobConfig {

    private String jobName;
    private Map<String, String> sessionConfig;

}

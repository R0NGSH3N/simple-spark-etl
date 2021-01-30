package com.r0ngsh3n.simplesparketl.job.config;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Setter
@Getter
public class JobConfig {

    private String jobName;

    private Map<String, String> sparkSessionOptions;

    private Boolean enableHiveSupport = false;
    private String sourceFormat;

    private String dbConnectionURL;
    private String dbTable;
    private String userName;
    private String password;

    private String inputCSVFileDir;
    private String outputCSVFileDir;

    private String extractorClassName;
    private String transformClassName;
    private String loaderClassName;

}

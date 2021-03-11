package com.r0ngsh3n.simplesparketl.job.config;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;


@Setter
@Getter
public class JobConfig implements Serializable {

    //Spark configuration
    private Boolean enableHiveSupport = false;
    private Map<String, String> jobArguments;

    private String sourceFormat;

    //DB Configuration
    private String dbConnectionURL;
    private String dbTable;
    private String userName;
    private String password;

    private String inputCSVFileDir;
    private String outputCSVFileDir;

    //hazelcast configuration
    private String instanceName;
    private String networkAddress;

    private String jobRunnerConfigureModuleClazzName;


}

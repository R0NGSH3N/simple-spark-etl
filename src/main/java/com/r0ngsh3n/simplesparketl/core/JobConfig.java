package com.r0ngsh3n.simplesparketl.core;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Setter
@Getter
public class JobConfig {

    private String jobName;

    private Map<String, String> sparkSessionOptions;

    private Boolean enableHiveSupport;
    private String sourceFormat;

    private String dbConnectionURL;
    private String dbTable;
    private String userName;
    private String password;

}

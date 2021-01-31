package com.r0ngsh3n.simplesparketl.config;

import java.util.List;

public class SparkConfig {
    private String home;
    private String master;

    private String bindingAddress;
    private String ports;
    private String executorMemory;
    private String extraJavaOptionsExecutor;
    private String deployMode;
    private boolean enableDebug;
    private String debugPort;

    private String mainClass;

    private String serviceJar;

    private String blockManagerPort;
    private String driverPort;
    private String driverMemory;

    private List<String> jars;

    private int getSparkRetryCount;
}

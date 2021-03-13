package com.r0ngsh3n.simplesparketl.job.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SparkConfig {
    private String home;
    private String master;
    private String jobName;
    private String jobConfigFileName;
    private Map<String, String> sparkSessionOptions;

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

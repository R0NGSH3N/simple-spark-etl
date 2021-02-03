package com.r0ngsh3n.simplesparketl.job.config;

import com.typesafe.config.Optional;
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
    private Map<String, String> sparkSessionOptions;

    @Optional
    private String bindingAddress;
    @Optional
    private String ports;
    @Optional
    private String executorMemory;
    @Optional
    private String extraJavaOptionsExecutor;
    @Optional
    private String deployMode;
    @Optional
    private boolean enableDebug;
    @Optional
    private String debugPort;

    private String mainClass;

    private String serviceJar;

    @Optional
    private String blockManagerPort;
    @Optional
    private String driverPort;
    @Optional
    private String driverMemory;

    @Optional
    private List<String> jars;

    @Optional
    private int getSparkRetryCount;
}

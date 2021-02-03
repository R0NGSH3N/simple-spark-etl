package com.r0ngsh3n.simplesparketl.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.util.List;

@Configuration
@PropertySource("classpath:spark-config-cluster.yml")
@ConfigurationProperties
@Getter
@Setter
public class SparkConfigCluster {
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

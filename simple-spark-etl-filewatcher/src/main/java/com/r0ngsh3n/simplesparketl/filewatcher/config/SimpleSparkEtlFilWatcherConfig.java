package com.r0ngsh3n.simplesparketl.filewatcher.config;

//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.r0ngsh3n.simplesparketl.job.config.SparkConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import com.typesafe.config.Optional;

import java.util.List;
import java.util.Map;

@Data
@Builder
@Slf4j
@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
public class SimpleSparkEtlFilWatcherConfig {
    private List<SourceConfig> sources;
    private SparkConfig sparkConfig;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SparkConfig{
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
    @Data
    @Builder(toBuilder = true)
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SourceConfig {
        public String directory;

        @Optional
        private long pollInSeconds;
        private String filePattern;
//        private String dataConfigYaml;

        @Optional
        private String destinationDir;

        @Optional
        private String DestinationFilePattern;

        @Optional
        private String destinationDB;
    }

    public static SimpleSparkEtlFilWatcherConfig loadConfig(){
        Config config = ConfigFactory.load();
        SimpleSparkEtlFilWatcherConfig simpleSparkEtlFilWatcherConfig = ConfigBeanFactory.create(config, SimpleSparkEtlFilWatcherConfig.class);

//        try {
//            log.info("{}", new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(simpleSparkEtlFilWatcherConfig));
//        } catch (JsonProcessingException e) {
//            throw new RuntimeException(e);
//        }

        return simpleSparkEtlFilWatcherConfig;
    }
}

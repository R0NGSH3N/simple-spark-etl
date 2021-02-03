package com.r0ngsh3n.simplesparketl.filewatcher.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.typesafe.config.Optional;

@Data
@Builder
@Slf4j
@NoArgsConstructor
@AllArgsConstructor
public class SimpleSparkEtlFilWatcherConfig {
//    private List<ExtractConfig> extractConfigDirectoryList;
    private SparkConfig sparkConfig;

    @Data
    @Builder(toBuilder = true)
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SourceConfig {
        public String directory;

        @Optional
        private long pollInSeconds;
        private String filePattern;
        private String dataConfigYaml;

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

        try {
            log.info("{}", new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(simpleSparkEtlFilWatcherConfig));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        return simpleSparkEtlFilWatcherConfig;
    }
}

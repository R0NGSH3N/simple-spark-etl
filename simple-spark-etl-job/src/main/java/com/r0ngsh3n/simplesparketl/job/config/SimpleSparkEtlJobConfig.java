package com.r0ngsh3n.simplesparketl.job.config;

//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.r0ngsh3n.simplesparketl.job.config.SparkConfig;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import com.typesafe.config.Optional;

import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Data
@Builder
@Slf4j
@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
public class SimpleSparkEtlJobConfig {
    private List<SourceConfig> sources;
    private SparkConfig sparkConfig;
    private CacheConfig cacheConfig;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CacheConfig{
        private String serviceURL;
        private String networkAddr;
    }

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
        private String destinationFilePattern;

        @Optional
        private String destinationDB;
    }

    @SneakyThrows
    public static SimpleSparkEtlJobConfig loadConfig(){
        Gson gson = new Gson();
        Reader reader = new InputStreamReader(Objects.requireNonNull(SimpleSparkEtlJobConfig.class.getResourceAsStream("/application.conf"), "Reading application.conf is Null."));
//        Reader reader = Files.newBufferedReader(Paths.get(configFilePath));
        JsonObject jsonObject = gson.fromJson(reader, JsonObject.class);

        SimpleSparkEtlJobConfig simpleSparkEtlJobConfig = SimpleSparkEtlJobConfig.builder().build();

        simpleSparkEtlJobConfig.sources = gson.fromJson(jsonObject.get("sourceConfig"), new TypeToken<List<SourceConfig>>(){}.getType());
        simpleSparkEtlJobConfig.sparkConfig = gson.fromJson(jsonObject.get("sparkConfig"), SparkConfig.class);
        simpleSparkEtlJobConfig.cacheConfig = gson.fromJson(jsonObject.get("cacheConfig"),CacheConfig.class);

        return simpleSparkEtlJobConfig;
    }
}

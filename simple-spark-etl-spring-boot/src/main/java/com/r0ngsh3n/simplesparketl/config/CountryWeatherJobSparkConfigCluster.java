package com.r0ngsh3n.simplesparketl.config;

import com.r0ngsh3n.simplesparketl.job.config.SparkConfig;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.util.HashMap;
import java.util.Map;

@Configuration
@PropertySource("classpath:spark-config-cluster.yml")
@ConfigurationProperties
@Getter
@Setter
public class CountryWeatherJobSparkConfigCluster {
    private String jobName;
    private String master = "Load Balancer";
    private Boolean enableDebug = false;
    private String bindAddress;
    private String ports;
    private String executorMemory = "8g";
    private String driverMemory = "16g";
    private String mainClass = "com.r0ngsh3n.simplesparketl.job.SimpleSparkEtlJobApplication";
    private String appResource ;

    @Bean(name = "clusterSparkConfig")
    public SparkConfig clusterSparkConfig(){
        SparkConfig sparkConfig = new SparkConfig();
        sparkConfig.setJobName(this.jobName);
        sparkConfig.setMaster(this.master);

        Map<String, String> sessionConfigs = new HashMap<>();
        sparkConfig.setSparkSessionOptions(sessionConfigs);
        return sparkConfig;

    }
}

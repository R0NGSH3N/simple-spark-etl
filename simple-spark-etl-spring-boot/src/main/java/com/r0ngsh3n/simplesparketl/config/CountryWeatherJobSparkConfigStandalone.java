package com.r0ngsh3n.simplesparketl.config;

import com.r0ngsh3n.simplesparketl.job.config.SparkConfig;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "spark-cluster")
@Getter
@Setter
public class CountryWeatherJobSparkConfigStandalone {
    private String jobName;
    private String sparkHome;
    private String master;
    private String serviceJar;
    private Boolean enableDebug = false;
    private String bindAddress;
    private String ports;
    private String executorMemory = "8g";
    private String driverMemory = "16g";
    private String mainClass = "com.r0ngsh3n.simplesparketl.job.SimpleSparkEtlJobApplication";
    private String appResource ;
    private List<String> jars;

    @Bean(name = "clusterSparkConfig")
    public SparkConfig clusterSparkConfig(){
        SparkConfig sparkConfig = new SparkConfig();
        sparkConfig.setJobName(this.jobName);
        sparkConfig.setMaster(this.master);
        sparkConfig.setServiceJar(this.serviceJar);
        sparkConfig.setJars(this.jars);
        sparkConfig.setHome(this.sparkHome);

        Map<String, String> sessionConfigs = new HashMap<>();
        sparkConfig.setSparkSessionOptions(sessionConfigs);
        return sparkConfig;

    }
}

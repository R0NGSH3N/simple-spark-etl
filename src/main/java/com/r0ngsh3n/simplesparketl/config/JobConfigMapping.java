package com.r0ngsh3n.simplesparketl.config;

import com.r0ngsh3n.simplesparketl.core.JobRunner;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.util.HashMap;
import java.util.Map;

@Configuration
@PropertySource("classpath:jobmapping.yml")
@ConfigurationProperties
@Getter
@Setter
public class JobConfigMapping {

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private Map<String, String> jobConfigs;

    @Bean
    public Map<String, JobRunner> jobMapping(){
        Map<String, JobRunner> jobMapping = new HashMap();
        for(String jobName : jobConfigs.keySet()){
            JobRunner jobRunner = applicationContext.getBean(jobConfigs.get(jobName));
            if( == null){
                throw new IllegalArgumentException
                        (String.format(" Job Runner %s for job Name s% does not exist !",
                                jobConfigs.get(jobName), jobName));
            }else{
                jobMapping.put(jobName, jobRunner);
            }
        }

        return jobMapping;


    }
}

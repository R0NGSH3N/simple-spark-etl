package com.r0ngsh3n.simplesparketl.config;

import com.google.common.base.Splitter;
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
    private String jobMappings;

    @Bean(name="jobMapping")
    public HashMap<String, JobRunner> jobMapping(){
        Map<String, String>  mapp = Splitter.on(",").withKeyValueSeparator("|").split(jobMappings);
        HashMap<String, JobRunner> jobMapping = new HashMap<>();
        for(String jobName : mapp.keySet()){
            JobRunner jobRunner = (JobRunner)applicationContext.getBean(mapp.get(jobName));
            if( jobRunner == null){
                throw new IllegalArgumentException
                        (String.format(" Job Runner %s for job Name s% does not exist !",
                                mapp.get(jobName), jobName));
            }else{
                jobMapping.put(jobName, jobRunner);
            }
        }

        return jobMapping;
    }

}

package com.r0ngsh3n.simplesparketl;

import com.r0ngsh3n.simplesparketl.config.JobConfigMapping;
import com.r0ngsh3n.simplesparketl.config.SampleJob;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(SampleJob.class)
public class SimpleSparkEtlApplication {

	public static void main(String[] args) {
		SpringApplication.run(SimpleSparkEtlApplication.class, args);
	}

}

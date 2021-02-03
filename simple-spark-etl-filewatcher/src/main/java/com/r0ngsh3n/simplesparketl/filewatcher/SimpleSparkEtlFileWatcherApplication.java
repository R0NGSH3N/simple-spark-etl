package com.r0ngsh3n.simplesparketl.filewatcher;

import com.r0ngsh3n.simplesparketl.filewatcher.config.SampleJobSparkConfigCluster;
import com.r0ngsh3n.simplesparketl.filewatcher.config.SimpleSparkEtlFilWatcherConfig;
import com.r0ngsh3n.simplesparketl.filewatcher.processors.SimpleSparkEtlProcessor;
import com.r0ngsh3n.simplesparketl.job.config.SparkConfig;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@Slf4j
@SpringBootApplication
//@SuppressWarnings("HideUtilityClassConstructor")
public class SimpleSparkEtlFileWatcherApplication implements CommandLineRunner {
    @Autowired
    private ApplicationContext applicationContext;

    public static void main(String[] args) {
        //remove the JUL
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        SpringApplication.run(SimpleSparkEtlFileWatcherApplication.class);
    }

    @Override
    public void run(String... args){
        SimpleSparkEtlFilWatcherConfig simpleSparkEtlFilWatcherConfig = SimpleSparkEtlFilWatcherConfig.loadConfig();
        SparkConfig sparkConfig = (SparkConfig)this.applicationContext.getBean("clusterSparkConfig");
        SimpleSparkEtlProcessor processor = new SimpleSparkEtlProcessor(simpleSparkEtlFilWatcherConfig, null, sparkConfig);
        processor.run();
    }


}


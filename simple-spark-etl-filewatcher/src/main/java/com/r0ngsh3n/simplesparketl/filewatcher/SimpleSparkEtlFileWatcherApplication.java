package com.r0ngsh3n.simplesparketl.filewatcher;

import com.r0ngsh3n.simplesparketl.filewatcher.config.SimpleSparkEtlFilWatcherConfig;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public final class SimpleSparkEtlFileWatcherApplication {
    private SimpleSparkEtlFileWatcherApplication() {
    }

    public static void main(String[] args) {
        //remove the JUL
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        SpringApplication.run(SimpleSparkEtlFileWatcherApplication.class);
    }

    public void run(String... args){
        SimpleSparkEtlFilWatcherConfig simpleSparkEtlFilWatcherConfig = SimpleSparkEtlFilWatcherConfig.loadConfig();


    }


}


package com.r0ngsh3n.simplesparketl.filewatcher.processors;

import com.r0ngsh3n.simplesparketl.job.config.SimpleSparkEtlJobConfig;
import com.r0ngsh3n.simplesparketl.job.core.submitter.SparkSubmitter;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.sql.Time;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Getter
public class SimpleSparkEtlProcessor {
    private final SimpleSparkEtlJobConfig simpleSparkEtlJobConfig;
    private final ReentrantLock lock;
    private final AtomicBoolean running;
    private ExecutorService executorService;

    private final List<SimpleSparkEtlJobConfig.SourceConfig> extractConfigDirectoryList;

    public SimpleSparkEtlProcessor(SimpleSparkEtlJobConfig config) {
        this.simpleSparkEtlJobConfig = config;
        this.lock = new ReentrantLock();
        this.running = new AtomicBoolean(true);
        this.extractConfigDirectoryList = config.getSourceConfigs();
        this.executorService = Executors.newFixedThreadPool(5);
    }

    private Function<String, CompletableFuture<String>> createSparkProcessor() {
        SparkSubmitter sparkSubmitter = new SparkSubmitter(this.simpleSparkEtlJobConfig);
        return dataFile -> sparkSubmitter.submit(dataFile);
    }

    public void run() {
        running.set(true);
//        Lock lock = locks.apply(simpleSparkEtlJobConfig);

        try {
            log.info("start the processors");

            while (running.get()) {
                if (acquireLock(lock)) {
                    List<CompletableFuture<Void>> futures = extractConfigDirectoryList.stream().map(sourceConfig -> CompletableFuture.runAsync(() -> {
                        runJob(sourceConfig);
                    }, executorService)).collect(Collectors.toList());

//                    CompletableFuture[] futures = extractConfigDirectoryList.stream().map(this::start)
//                            .toArray(CompletableFuture[]::new);
                    CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
                    lock.unlock();
                }
            }

        } finally {
            log.info("run into finally. we are exiting the processor.");
            running.set(false);
            lock.unlock();
        }
    }

    public void runJob(SimpleSparkEtlJobConfig.SourceConfig sourceConfig){
        log.info("start directory monitoring job, directory: {}, Thread name: {}", sourceConfig.directory, Thread.currentThread().getName());
        Function<String, CompletableFuture<String>> sparkSubmitter = createSparkProcessor();
        FileProcessor fileProcessor = new FileProcessor(sourceConfig, sparkSubmitter);
        fileProcessor.process();
        log.info("complete directory monitoring job, directory: {}, Thread name: {}", sourceConfig.directory, Thread.currentThread().getName());

    }

    public Boolean acquireLock(Lock lock) {
        try {
            return lock.tryLock();
        } catch (Exception e) {
            log.info("{}", e);
            return false;
        }
    }

}

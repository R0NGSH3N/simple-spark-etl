package com.r0ngsh3n.simplesparketl.filewatcher.processors;

import com.r0ngsh3n.simplesparketl.filewatcher.config.SimpleSparkEtlFilWatcherConfig;
import com.r0ngsh3n.simplesparketl.job.config.SparkConfig;
import com.r0ngsh3n.simplesparketl.job.core.submitter.SparkSubmitter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class SimpleSparkEtlProcessor {
    private final SimpleSparkEtlFilWatcherConfig simpleSparkEtlFilWatcherConfig;
    private final SparkConfig sparkConfig;
    private final Function<SimpleSparkEtlFilWatcherConfig, Lock> locks;
    private final AtomicBoolean running;
    private ExecutorService executorService;

    private final List<SimpleSparkEtlFilWatcherConfig.SourceConfig> extractConfigDirectoryList;

    public SimpleSparkEtlProcessor(SimpleSparkEtlFilWatcherConfig config, Function<SimpleSparkEtlFilWatcherConfig, Lock> locks, SparkConfig sparkConfig) {
        this.simpleSparkEtlFilWatcherConfig = config;
        this.locks = locks == null ? t -> new ReentrantLock() : locks;
        this.running = new AtomicBoolean(true);
        this.sparkConfig = sparkConfig;
        this.extractConfigDirectoryList = config.getExtractConfigDirectoryList();
    }

    /**
    private CompletableFuture start(SimpleSparkEtlFilWatcherConfig.ExtractConfig extractConfig) {
        return CompletableFuture.runAsync(() -> {
            Function<String, CompletableFuture<String>> sparkSubmitter = createSparkProcessor(extractConfig);
            FileProcessor fileProcessor = new FileProcessor(extractConfig, sparkSubmitter);
            fileProcessor.process();
        }, executorService);
    }
     **/

    private Function<String, CompletableFuture<String>> createSparkProcessor() {
        SparkSubmitter sparkSubmitter = new SparkSubmitter(this.sparkConfig);
        return dataFile -> sparkSubmitter.submit(dataFile);
    }

    public void run() {
        running.set(true);
        Lock lock = locks.apply(simpleSparkEtlFilWatcherConfig);

        try {
            log.info("start watching folder... {}", Thread.currentThread().getName());

            while (running.get()) {
                if (acquireLock(lock)) {
                    extractConfigDirectoryList.stream().map(sourceConfig -> CompletableFuture.runAsync(() -> {
                        Function<String, CompletableFuture<String>> sparkSubmitter = createSparkProcessor();
                        FileProcessor fileProcessor = new FileProcessor(sourceConfig, sparkSubmitter);
                        fileProcessor.process();
                    }, executorService)).collect(Collectors.toList()).stream().map(CompletableFuture::join).collect(Collectors.toList());

//                    CompletableFuture[] futures = extractConfigDirectoryList.stream().map(this::start)
//                            .toArray(CompletableFuture[]::new);
//                    CompletableFuture.allOf(futures).join();
                    lock.unlock();
                }
            }
        } finally {
            running.set(false);
            lock.unlock();
        }
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

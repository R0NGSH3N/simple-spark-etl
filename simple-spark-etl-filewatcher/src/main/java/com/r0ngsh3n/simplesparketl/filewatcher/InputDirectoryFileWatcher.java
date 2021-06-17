package com.r0ngsh3n.simplesparketl.filewatcher;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
public class InputDirectoryFileWatcher {
    private String inputDirectory;
    private InputDataFileHandler inputDataFileHandler;
    private Long pollingInterrval;

    public void startUo() throws Exception {
        log.info("File Watcher started thread Id {}, Input Directory: {} ", Thread.currentThread().getName(), this.inputDirectory);
        Path path = Paths.get(inputDirectory);
        if (!Files.exists(path)) {
            log.info("Input Directory {} does not exists, we will create ", inputDirectory);
            path = Files.createDirectories(path);
            if (!path.toString().equals(inputDirectory)) {
                throw new RuntimeException(String.format("can not create Input directory %s ", inputDirectory));
            }
        }

        File[] fileArray = path.toFile().listFiles();
        if (fileArray != null && fileArray.length > 0) {
            for (File dataFile : fileArray) {
                if (dataFile != null && dataFile.isFile()) {
                    log.info(" data file found {}", dataFile.getCanonicalPath());
                    try {
                        this.inputDataFileHandler.handleFile(dataFile);
                    } catch (Exception e) {
                        log.error(" Error in handleFile: {} ", e);
                    }
                }
            }
        }

        //start file watcher demon
        FileAlterationListener fileAlterationListener = new FileAlterationListener() {

            @Override
            public void onStart(FileAlterationObserver observer) {
            }

            @Override
            public void onDirectoryCreate(File directory) {
            }

            @Override
            public void onDirectoryChange(File directory) {
            }

            @Override
            public void onDirectoryDelete(File directory) {
            }

            @SneakyThrows
            @Override
            public void onFileCreate(File file) {
                log.info("new data file found: {}", file.getCanonicalPath());
                inputDataFileHandler.handleFile(file);
            }

            @SneakyThrows
            @Override
            public void onFileChange(File file) {
                log.info("data file changed : {}", file.getCanonicalPath());
            }

            @SneakyThrows
            @Override
            public void onFileDelete(File file) {
                log.info("data file deleted: {}", file.getCanonicalPath());
            }

            @Override
            public void onStop(FileAlterationObserver observer) {
            }
        };

        FileAlterationObserver fileAlterationObserver = new FileAlterationObserver(path.toFile());
        fileAlterationObserver.addListener(fileAlterationListener);
        FileAlterationMonitor fileAlterationMonitor = new FileAlterationMonitor(pollingInterrval);
        fileAlterationMonitor.addObserver(fileAlterationObserver);
        fileAlterationMonitor.start();
    }
}

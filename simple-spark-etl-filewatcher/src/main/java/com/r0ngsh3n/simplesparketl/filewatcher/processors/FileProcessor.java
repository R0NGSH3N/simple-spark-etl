package com.r0ngsh3n.simplesparketl.filewatcher.processors;

import com.r0ngsh3n.simplesparketl.filewatcher.config.SimpleSparkEtlFilWatcherConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@Slf4j
public class FileProcessor {
    private static final String ARCHIVIVE_DIR = "";
    private final String directory ;
    private final Pattern filePattern;

    private Function<String, CompletableFuture<String>> fileHandler;

    public FileProcessor(SimpleSparkEtlFilWatcherConfig.SourceConfig sourceConfig, Function<String, CompletableFuture<String>> fileHandler){
        this.fileHandler = fileHandler;
        this.directory = sourceConfig.getDirectory();
        this.filePattern = Pattern.compile(sourceConfig.getFilePattern());

    }

    public void process(){
        try {
            Path path = getWatchPath();
            scanDirectory(path).map(File::getAbsolutePath)
                    .filter(this::findNonTextFile)
                    .findFirst()
                    .ifPresent(filePath -> processFile(filePath, fileHandler.apply(filePath)));
        } catch (IOException e) {
//            e.printStackTrace();
            log.error("IOException {}", e);
        }

    }

    public void processFile(String filePath, CompletableFuture<String> jobFuture){
        try {
            Instant start = Instant.now();
            String fileSize = FileUtils.byteCountToDisplaySize(new File(filePath).length());
            log.info("submit file to spark: {}", filePath);
            String state = jobFuture.get();
            log.info("Job done with status: {}, file: {}, size: {}, process time in seconds: {} ", state, filePath, fileSize, Duration.between(start, Instant.now()).getSeconds());
        } catch (InterruptedException | ExecutionException e ) {
            e.printStackTrace();
            log.error("fiel to process file {} with messaage {}", filePath, e.getMessage());
        }
    }

    private boolean findNonTextFile(String filePath){
        try {
            String type = Files.probeContentType(Paths.get(filePath));
            if (type == null) {
                //type couldn't be determined, assume binary
                return true;
            } else if (type.startsWith("text")) {
                return false;
            } else {
                //type isn't text
                return true;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Stream<File> scanDirectory(Path directory){
        File[] files = directory.toFile().listFiles(this::fileFilter);
        if(files == null){
            throw new RuntimeException("can not list files for " + directory);
        }
        return Arrays.stream(files);
    }

    private boolean fileFilter(File file){
        return file.isFile() &&
                filePattern.matcher(file.getName()).find();
    }

    private Path getWatchPath() throws IOException{
        Path path = Paths.get(directory);
        if(Files.notExists(path)){
            log.info("path does not exist, create : {} ", path);
            Files.createDirectories(path);
        }

        return path;
    }



}

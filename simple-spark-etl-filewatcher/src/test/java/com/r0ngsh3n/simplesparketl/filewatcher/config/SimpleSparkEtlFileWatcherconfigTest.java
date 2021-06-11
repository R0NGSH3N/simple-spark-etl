package com.r0ngsh3n.simplesparketl.filewatcher.config;

import com.r0ngsh3n.simplesparketl.job.config.SimpleSparkEtlJobConfig;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SimpleSparkEtlFileWatcherconfigTest {

    @Test
    public void testLoadConfig(){
        SimpleSparkEtlJobConfig simpleSparkEtlJobConfig = SimpleSparkEtlJobConfig.loadConfig();
        List<SimpleSparkEtlJobConfig.SourceConfig> sourceConfigs = simpleSparkEtlJobConfig.getSources();
        SimpleSparkEtlJobConfig.SparkConfig sparkConfig = simpleSparkEtlJobConfig.getSparkConfig();
        SimpleSparkEtlJobConfig.CacheConfig cacheConfig = simpleSparkEtlJobConfig.getCacheConfig();

        SimpleSparkEtlJobConfig.SourceConfig firstSourceConfig = sourceConfigs.get(0);
        SimpleSparkEtlJobConfig.SourceConfig secondSourceConfig = sourceConfigs.get(1);

        assertEquals(firstSourceConfig.getDirectory(), "first monitoring directory");
        assertEquals(firstSourceConfig.getFilePattern(), "\\.csv$");
        assertEquals(firstSourceConfig.getPollInSeconds(), 300);
        assertEquals(firstSourceConfig.getDestinationDB(), "some jdbc connection URL");
        assertNull(firstSourceConfig.getDestinationDir());
        assertNull(firstSourceConfig.getDestinationFilePattern());

        assertEquals(secondSourceConfig.getDirectory(), "second monitoring directory");
        assertEquals(secondSourceConfig.getFilePattern(), "\\.csv$");
        assertEquals(secondSourceConfig.getPollInSeconds(), 500);
        assertNull(secondSourceConfig.getDestinationDB());
        assertEquals(secondSourceConfig.getDestinationDir(), "Destination Directory");
        assertEquals(secondSourceConfig.getDestinationFilePattern(), "\\.csv$");

        assertEquals(sparkConfig.getHome(), "home");
        assertEquals(sparkConfig.getMaster(), "master");
        assertEquals(sparkConfig.getExecutorMemory(), "4g");
        assertEquals(sparkConfig.getDriverMemory(), "4g");
        assertEquals(sparkConfig.getGetSparkRetryCount(), 5);
        assertEquals(sparkConfig.getMainClass(), "mainClass");
        assertEquals(sparkConfig.getServiceJar(), "sourceJar");
        assertEquals(sparkConfig.getJars().size(), 3);
        List<String> jars = sparkConfig.getJars();
        assertEquals(jars.get(0),"jar1");
        assertEquals(jars.get(1),"jar2");
        assertEquals(jars.get(2),"jar3");

        assertEquals(cacheConfig.getServiceURL(), "something");
        assertEquals(cacheConfig.getNetworkAddr(), "127.0.0.1");

    }

}

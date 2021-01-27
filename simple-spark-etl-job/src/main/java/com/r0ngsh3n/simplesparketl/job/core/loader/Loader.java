package com.r0ngsh3n.simplesparketl.job.core.loader;

import com.r0ngsh3n.simplesparketl.job.core.JobContext;

public interface Loader<T> {

    public void load(JobContext<T> jobContext);
}

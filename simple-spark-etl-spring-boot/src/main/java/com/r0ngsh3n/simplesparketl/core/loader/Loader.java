package com.r0ngsh3n.simplesparketl.core.loader;

import com.r0ngsh3n.simplesparketl.core.JobContext;

public interface Loader<T> {

    public void load(JobContext<T> jobContext);
}

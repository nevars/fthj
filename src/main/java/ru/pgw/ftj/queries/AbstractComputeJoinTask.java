package ru.pgw.ftj.queries;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

public abstract class AbstractComputeJoinTask extends ComputeTaskAdapter<Object, Object> {

    @IgniteInstanceResource
    protected Ignite ignite;

    @LoggerResource
    protected IgniteLogger log;

    protected String logPrefix;

}

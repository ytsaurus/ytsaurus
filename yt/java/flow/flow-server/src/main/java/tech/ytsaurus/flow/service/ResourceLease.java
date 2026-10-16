package tech.ytsaurus.flow.service;

import java.util.List;
import java.util.Map;

import tech.ytsaurus.flow.resource.FlowResource;

/**
 * Keeps the instances a batch resolved usable until it releases them.
 *
 * <p>Release it once the batch is done, whatever happened to the resources meanwhile.
 */
public final class ResourceLease implements AutoCloseable {
    static final ResourceLease EMPTY = new ResourceLease(Map.of(), List.of());
    private final Map<String, FlowResource> resources;
    private volatile List<ResourceInstance> instances;

    ResourceLease(Map<String, FlowResource> resources, List<ResourceInstance> instances) {
        this.resources = Map.copyOf(resources);
        // The store transfers this private list; no caller mutates it afterwards.
        this.instances = instances;
    }

    /**
     * Returns the acquired instances keyed by the alias the job refers to them by.
     *
     * @return the acquired resources; empty when no reference carried an alias.
     */
    public Map<String, FlowResource> resources() {
        return resources;
    }

    /**
     * Drops every reference this lease holds; idempotent.
     */
    public void release() {
        if (instances.isEmpty()) {
            return;
        }
        List<ResourceInstance> released;
        synchronized (this) {
            released = instances;
            instances = List.of();
        }
        ResourceInstance.releaseAll(released);
    }

    @Override
    public void close() {
        release();
    }
}

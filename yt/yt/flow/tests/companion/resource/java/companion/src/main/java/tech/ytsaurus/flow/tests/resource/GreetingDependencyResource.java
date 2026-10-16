package tech.ytsaurus.flow.tests.resource;

import tech.ytsaurus.flow.resource.FlowResource;
import tech.ytsaurus.flow.resource.ResourceContext;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Dependency resource under test: exposes the {@code value} dynamic parameter,
 * refreshed on every reconfigure delivered by the worker.
 */
public class GreetingDependencyResource implements FlowResource {

    private volatile ResourceContext context;

    static String stringParameter(YTreeNode parameters, String key) {
        YTreeNode child = parameters.asMap().get(key);
        return child == null ? "" : child.stringValue();
    }

    @Override
    public void load(ResourceContext context) {
        this.context = context;
    }

    @Override
    public void reconfigure(ResourceContext context) {
        this.context = context;
    }

    @Override
    public void unload() {
        // This resource holds only JVM data; there are no external resources to release.
    }

    public String getValue() {
        return stringParameter(context.dynamicParameters(), "value");
    }
}

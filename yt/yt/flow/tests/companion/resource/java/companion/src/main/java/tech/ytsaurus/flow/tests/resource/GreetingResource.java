package tech.ytsaurus.flow.tests.resource;

import tech.ytsaurus.flow.resource.FlowResource;
import tech.ytsaurus.flow.resource.ResourceContext;

/**
 * Companion-hosted resource under test: load records the pid of the serving
 * companion process and the dependency value; a reconfigure delivered by the
 * worker is observable as a fresh suffix value, a dependency change as a fresh
 * dependency value after the worker re-inits this resource with the advanced
 * dependency reference.
 */
public class GreetingResource implements FlowResource {

    private volatile ResourceContext context;
    private volatile String dependencyValue = "";
    private volatile long initPid = -1;

    @Override
    public void load(ResourceContext context) {
        this.context = context;
        this.dependencyValue = context.dependency("dependency", GreetingDependencyResource.class).getValue();
        this.initPid = ProcessHandle.current().pid();
    }

    @Override
    public void reconfigure(ResourceContext context) {
        this.context = context;
    }

    @Override
    public void unload() {
        // This resource holds only JVM data; there are no external resources to release.
    }

    public String getGreeting() {
        return GreetingDependencyResource.stringParameter(context.parameters(), "greeting");
    }

    public String getSuffix() {
        return GreetingDependencyResource.stringParameter(context.dynamicParameters(), "suffix");
    }

    public String getDependencyValue() {
        return dependencyValue;
    }

    public long getInitPid() {
        return initPid;
    }
}

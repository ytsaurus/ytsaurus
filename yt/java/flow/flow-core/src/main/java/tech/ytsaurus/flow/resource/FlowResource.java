package tech.ytsaurus.flow.resource;

/**
 * Base contract for companion-hosted resources.
 *
 * <p>A companion resource is declared in the pipeline spec as a resource whose
 * {@code parameters} name the companion-side class under the
 * {@code companion_resource_class} key. The worker drives the hosted instance
 * through ResourceExecute commands (init/unload); computations reach it via
 * {@link tech.ytsaurus.flow.context.RuntimeContext#getResource(String)} by the
 * alias from the computation's {@code required_resource_ids} entry.
 *
 * <p>One instance is shared by every job that requires it, so it must tolerate
 * concurrent use.
 */
public interface FlowResource {

    /**
     * Builds the instance from the resource parameters, dynamic parameters, and
     * dependency instances carried by the context.
     *
     * <p>Wrap expected checked failures in {@link ResourceLoadException}, preserving the cause.
     * A failed load is followed by {@link #unload()}, including after partial initialization.
     *
     * @param context the resource context to load from.
     * @throws ResourceLoadException if loading fails; reported to the worker in-band for retry.
     */
    void load(ResourceContext context) throws ResourceLoadException;

    /**
     * Applies an updated dynamic spec to the live instance.
     *
     * <p>May overlap use by batches and dependent resources that already acquired this instance.
     * Implementations must synchronize their own mutable state; batch leases prevent unload, not
     * concurrent reconfiguration. New acquisitions are refused until this hook returns.
     *
     * <p>The default keeps the instance as is; read the refreshed values from
     * the context passed here. On exception the instance is unloaded and
     * dropped, and the worker's retry rebuilds it via
     * {@link #load(ResourceContext)}.
     *
     * @param context the resource context carrying the new dynamic parameters.
     * @throws Exception if reconfiguration fails.
     */
    default void reconfigure(ResourceContext context) throws Exception {
    }

    /**
     * Releases whatever the instance holds outside the process.
     *
     * <p>Runs once nothing holds the instance any more, and on an instance whose
     * {@link #load(ResourceContext)} raised partway, so it must tolerate partial
     * initialization. Flow attempts unload once for every instance returned by the factory.
     * An {@link Exception} it raises is logged and swallowed; anything else propagates.
     * Declaring {@code throws Exception} lets an implementation delegate straight to a
     * checked-exception teardown such as {@link java.io.Closeable#close()}.
     *
     * <p>Every implementation must explicitly define its cleanup. A resource that holds no
     * external resources can implement a no-op.
     *
     * @throws Exception if the teardown fails; logged and swallowed by the store.
     */
    void unload() throws Exception;
}

package tech.ytsaurus.flow.service;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.google.protobuf.ByteString;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.context.PipelineContextSnapshot;
import tech.ytsaurus.flow.internal.resource.CompanionResourceInstanceReference;
import tech.ytsaurus.flow.resource.FlowResource;
import tech.ytsaurus.flow.resource.ResourceContext;
import tech.ytsaurus.flow.resource.ResourceLoadException;
import tech.ytsaurus.flow.rpc.EResourceCommand;
import tech.ytsaurus.flow.rpc.EResourceExecuteStatus;
import tech.ytsaurus.flow.utils.YsonUtils;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link ResourceStore}; mirrors the C++ resource_store_ut and the Python
 * test_resource_store cases.
 */
@DisplayName("ResourceStore Tests")
public class ResourceStoreTest {

    private static final GUID INCARNATION_A = GUID.valueOf("1-2-3-4");
    private static final GUID INCARNATION_B = GUID.valueOf("5-6-7-8");

    @BeforeEach
    void setUp() {
        TrackingResource.INSTANCES.clear();
    }

    private static ResourceStore makeStore() {
        return makeStore(Map.of("TrackingResource", TrackingResource::new));
    }

    private static ResourceStore makeStore(Map<String, Supplier<? extends FlowResource>> factories) {
        return new ResourceStore(factories);
    }

    private static InitArg initArg() {
        return new InitArg();
    }

    private static ByteString unloadArg(GUID incarnationId) {
        return YsonUtils.protoFromYTree(YTree.builder().beginMap()
                .key("incarnation_id").value(incarnationId.toString())
                .endMap().build());
    }

    /**
     * Returns the instance the store serves for the reference, or {@code null}, through the
     * production acquire path — which, unlike a bare lookup, also requires the handle to still
     * hold a live reference.
     */
    private static @Nullable FlowResource acquiredResource(
            ResourceStore store,
            CompanionResourceInstanceReference reference
    ) {
        var probe = new CompanionResourceInstanceReference(
                reference.resourceId(),
                reference.incarnationId(),
                reference.configurationGeneration(),
                "probe"
        );
        ResourceLease lease = store.acquire(List.of(probe));
        if (lease == null) {
            return null;
        }
        try {
            return lease.resources().get("probe");
        } finally {
            lease.release();
        }
    }

    private static CompanionResourceInstanceReference reference(String resourceId) {
        return reference(resourceId, INCARNATION_A, 0, null);
    }

    private static CompanionResourceInstanceReference reference(
            String resourceId,
            GUID incarnationId,
            long configurationGeneration,
            @Nullable String alias
    ) {
        return new CompanionResourceInstanceReference(resourceId, incarnationId, configurationGeneration, alias);
    }

    private static YTreeNode dependencyNode(String resourceId) {
        return dependencyNode(resourceId, INCARNATION_A, 0, null);
    }

    private static YTreeNode dependencyNode(
            String resourceId,
            GUID incarnationId,
            long configurationGeneration,
            @Nullable String alias
    ) {
        YTreeBuilder builder = YTree.builder().beginMap()
                .key("resource_id").value(resourceId)
                .key("incarnation_id").value(incarnationId.toString())
                .key("configuration_generation").value(configurationGeneration);
        if (alias != null) {
            builder.key("alias").value(alias);
        }
        return builder.endMap().build();
    }

    @Test
    @DisplayName("Init once: the very same init converges without rebuilding the instance")
    void initOnceAndConvergeNoOp() {
        var store = makeStore();
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()).status());
        assertEquals(1, TrackingResource.INSTANCES.size());
        assertTrue(TrackingResource.INSTANCES.get(0).loaded);

        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()).status());
        assertEquals(1, TrackingResource.INSTANCES.size());

        assertSame(TrackingResource.INSTANCES.get(0), acquiredResource(store, reference("r")));
    }

    @Test
    void oneRegisteredTypeCreatesIndependentResourcesForDifferentIds() {
        var context = new PipelineContext();
        context.registerResourceClass("TrackingResource", TrackingResource::new);
        var store = new ResourceStore(new PipelineContextSnapshot(context).getResourceFactories());
        assertTrue(TrackingResource.INSTANCES.isEmpty());
        try {
            var english = initArg().setParameters(Map.of("greeting", "hello"));
            var russian = initArg().setParameters(Map.of("greeting", "privet"));
            assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "english", english).status());
            assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "russian", russian).status());
            var first = (TrackingResource) acquiredResource(store, reference("english"));
            var second = (TrackingResource) acquiredResource(store, reference("russian"));
            assertNotNull(first);
            assertNotNull(second);
            assertNotSame(first, second);
            assertEquals("english", first.context.resourceId());
            assertEquals("russian", second.context.resourceId());
            assertEquals("hello", first.context.parameters().asMap().get("greeting").stringValue());
            assertEquals("privet", second.context.parameters().asMap().get("greeting").stringValue());
            assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "english", english).status());
            assertEquals(2, TrackingResource.INSTANCES.size());
        } finally {
            assertTrue(store.shutdown());
        }
        assertTrue(TrackingResource.INSTANCES.stream().allMatch(instance -> instance.unloaded));
    }

    @Test
    void registeredFactoryCreatesAFreshInstanceWhileThePreviousIncarnationIsLeased() {
        var context = new PipelineContext();
        context.registerResourceClass("TrackingResource", TrackingResource::new);
        var store = new ResourceStore(new PipelineContextSnapshot(context).getResourceFactories());
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()).status());
        var oldLease = store.acquire(List.of(reference("r", INCARNATION_A, 0, "view")));
        assertNotNull(oldLease);
        try {
            var old = (TrackingResource) oldLease.resources().get("view");
            assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()
                    .setIncarnationId(INCARNATION_B).setIncarnationGeneration(2)).status());
            var current = (TrackingResource) acquiredResource(store, reference("r", INCARNATION_B, 0, null));
            assertNotNull(current);
            assertNotSame(old, current);
            assertEquals(2, TrackingResource.INSTANCES.size());
            assertFalse(old.unloaded);
            oldLease.close();
            assertTrue(old.unloaded);
            assertFalse(current.unloaded);
        } finally {
            oldLease.close();
            assertTrue(store.shutdown());
        }
    }

    @Test
    @DisplayName("Load receives the static and dynamic parameters")
    void loadReceivesParameters() {
        var store = makeStore();
        var argument = initArg()
                .setParameters(Map.of("greeting", "hello"))
                .setDynamicParameters(Map.of("suffix", "v1"));
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", argument).status());

        ResourceContext context = TrackingResource.INSTANCES.get(0).context;
        assertNotNull(context);
        assertEquals("r", context.resourceId());
        assertEquals("hello", context.parameters().asMap().get("greeting").stringValue());
        assertEquals("v1", context.dynamicParameters().asMap().get("suffix").stringValue());
    }

    @Test
    @DisplayName("Changed dynamic spec: the same instance is reconfigured, not replaced")
    void convergeOnChangedDynamicSpec() {
        var store = makeStore();
        assertEquals(
                EResourceExecuteStatus.RES_OK,
                execute(store, "r", initArg().setDynamicParameters(Map.of("v", "1"))).status());
        var outcome = execute(store, "r", initArg()
                .setConfigurationGeneration(1)
                .setDynamicParameters(Map.of("v", "2")));
        assertEquals(EResourceExecuteStatus.RES_OK, outcome.status());
        // The same instance was reconfigured, not replaced.
        assertEquals(1, TrackingResource.INSTANCES.size());
        assertEquals(1, TrackingResource.INSTANCES.get(0).reconfigureCount);

        // The old generation reference no longer matches; the new one does.
        assertNull(acquiredResource(store, reference("r", INCARNATION_A, 0, null)));
        assertNotNull(acquiredResource(store, reference("r", INCARNATION_A, 1, null)));
    }

    @Test
    @DisplayName("Static spec change within one incarnation is rejected")
    void staticSpecChangeRejected() {
        var store = makeStore();
        assertEquals(
                EResourceExecuteStatus.RES_OK,
                execute(store, "r", initArg().setParameters(Map.of("a", "1"))).status());
        var outcome = execute(store, "r", initArg()
                .setConfigurationGeneration(1)
                .setParameters(Map.of("a", "2")));
        assertEquals(EResourceExecuteStatus.RES_ERROR, outcome.status());
        assertTrue(outcome.errorMessage().contains("Static resource spec changed"));
    }

    @Test
    @DisplayName("Conflicting dynamic specs at the same generation are rejected")
    void conflictingDynamicSpecsAtSameGenerationRejected() {
        var store = makeStore();
        assertEquals(
                EResourceExecuteStatus.RES_OK,
                execute(store, "r", initArg().setDynamicParameters(Map.of("v", "1"))).status());
        var outcome = execute(store, "r", initArg().setDynamicParameters(Map.of("v", "2")));
        assertEquals(EResourceExecuteStatus.RES_ERROR, outcome.status());
        assertTrue(outcome.errorMessage().contains("conflicting dynamic specs"));
    }

    @Test
    @DisplayName("Unload bars new acquisition, calls the unload hook, and is idempotent")
    void unloadBarsNewAcquisition() {
        var store = makeStore();
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()).status());
        assertEquals(EResourceExecuteStatus.RES_OK, unload(store, "r", INCARNATION_A).status());
        assertNull(acquiredResource(store, reference("r")));
        assertTrue(TrackingResource.INSTANCES.get(0).unloaded);

        // Unload is idempotent.
        assertEquals(EResourceExecuteStatus.RES_OK, unload(store, "r", INCARNATION_A).status());
    }

    @Test
    @DisplayName("A retired incarnation cannot be revived")
    void retiredIncarnationCannotBeRevived() {
        var store = makeStore();
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()).status());
        assertEquals(EResourceExecuteStatus.RES_OK, unload(store, "r", INCARNATION_A).status());
        var outcome = execute(store, "r", initArg());
        assertEquals(EResourceExecuteStatus.RES_STALE_RESOURCE_INCARNATION, outcome.status());
    }

    @Test
    @DisplayName("Re-init after unload creates a fresh instance under the new incarnation")
    void reinitAfterUnloadCreatesFreshInstance() {
        var store = makeStore();
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()).status());
        assertEquals(EResourceExecuteStatus.RES_OK, unload(store, "r", INCARNATION_A).status());
        var outcome = execute(store, "r", initArg()
                .setIncarnationId(INCARNATION_B)
                .setIncarnationGeneration(2));
        assertEquals(EResourceExecuteStatus.RES_OK, outcome.status());
        assertEquals(2, TrackingResource.INSTANCES.size());
        assertNotNull(acquiredResource(store, reference("r", INCARNATION_B, 0, null)));
    }

    @Test
    @DisplayName("A late init of an older incarnation does not displace the newer one")
    void outOfOrderInitConvergesToNewestIncarnation() {
        var store = makeStore();
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()
                .setIncarnationId(INCARNATION_B)
                .setIncarnationGeneration(2)).status());
        // A late init of the older incarnation must not displace the newer one.
        var outcome = execute(store, "r", initArg().setIncarnationGeneration(1));
        assertEquals(EResourceExecuteStatus.RES_STALE_RESOURCE_INCARNATION, outcome.status());
        assertNotNull(acquiredResource(store, reference("r", INCARNATION_B, 0, null)));
    }

    @Test
    @DisplayName("A late unload of an older incarnation cannot retire its successor")
    void lateUnloadCannotRetireSuccessor() {
        var store = makeStore();
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()).status());
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()
                .setIncarnationId(INCARNATION_B)
                .setIncarnationGeneration(2)).status());
        // A late unload of the older incarnation is a no-op.
        assertEquals(EResourceExecuteStatus.RES_OK, unload(store, "r", INCARNATION_A).status());
        assertNotNull(acquiredResource(store, reference("r", INCARNATION_B, 0, null)));
    }

    @Test
    @DisplayName("A tombstone fences only its own incarnation, not future successors")
    void mismatchingUnloadDoesNotFenceFutureSuccessor() {
        var store = makeStore();
        assertEquals(EResourceExecuteStatus.RES_OK, unload(store, "r", INCARNATION_A).status());
        // The tombstone fences only its own incarnation.
        var outcome = execute(store, "r", initArg()
                .setIncarnationId(INCARNATION_B)
                .setIncarnationGeneration(2));
        assertEquals(EResourceExecuteStatus.RES_OK, outcome.status());
    }

    @Test
    @DisplayName("Unload before init creates a tombstone fencing the incarnation")
    void unloadBeforeInitCreatesTombstone() {
        var store = makeStore();
        assertEquals(EResourceExecuteStatus.RES_OK, unload(store, "r", INCARNATION_A).status());
        var outcome = execute(store, "r", initArg());
        assertEquals(EResourceExecuteStatus.RES_STALE_RESOURCE_INCARNATION, outcome.status());
    }

    @Test
    @DisplayName("An older configuration generation converges without touching the newer state")
    void configurationGenerationsConverge() {
        var store = makeStore();
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()
                .setConfigurationGeneration(2)
                .setDynamicParameters(Map.of("v", "2"))).status());
        // An older generation converges without touching the newer state.
        var outcome = execute(store, "r", initArg()
                .setConfigurationGeneration(1)
                .setDynamicParameters(Map.of("v", "1")));
        assertEquals(EResourceExecuteStatus.RES_OK, outcome.status());
        assertNotNull(acquiredResource(store, reference("r", INCARNATION_A, 2, null)));
        assertEquals(1, TrackingResource.INSTANCES.size());
    }

    @Test
    @DisplayName("A failed reconfigure quarantines the instance; the retry rebuilds it")
    void failedReconfigureQuarantinesAndRecreatesResource() {
        var store = makeStore(Map.of("TrackingResource", () -> new TrackingResource(false, true)));
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()).status());
        var outcome = execute(store, "r", initArg()
                .setConfigurationGeneration(1)
                .setDynamicParameters(Map.of("v", "2")));
        assertEquals(EResourceExecuteStatus.RES_ERROR, outcome.status());
        assertNull(acquiredResource(store, reference("r")));
        // The retry of the same init rebuilds a fresh instance.
        outcome = execute(store, "r", initArg()
                .setConfigurationGeneration(1)
                .setDynamicParameters(Map.of("v", "2")));
        assertEquals(EResourceExecuteStatus.RES_OK, outcome.status());
        assertEquals(2, TrackingResource.INSTANCES.size());
        assertNotNull(acquiredResource(store, reference("r", INCARNATION_A, 1, null)));
    }

    @Test
    @DisplayName("A failed reconfigure keeps the incarnation's static spec immutable")
    void failedReconfigureKeepsStaticSpecImmutable() {
        var store = makeStore(Map.of("TrackingResource", () -> new TrackingResource(false, true)));
        assertEquals(
                EResourceExecuteStatus.RES_OK,
                execute(store, "r", initArg().setParameters(Map.of("a", "1"))).status());
        // Quarantines the instance without advancing the published generation.
        assertEquals(
                EResourceExecuteStatus.RES_ERROR,
                execute(store, "r", initArg()
                        .setConfigurationGeneration(1)
                        .setParameters(Map.of("a", "1"))
                        .setDynamicParameters(Map.of("v", "2"))).status());

        // The failure must not have erased the static-spec history: a later init at this
        // incarnation carrying a different static spec is still rejected rather than rebuilt.
        var outcome = execute(store, "r", initArg()
                .setConfigurationGeneration(1)
                .setParameters(Map.of("a", "2")));
        assertEquals(EResourceExecuteStatus.RES_ERROR, outcome.status());
        assertTrue(outcome.errorMessage().contains("Static resource spec changed"));
    }

    @Test
    @DisplayName("An init argument omitting a required spec is rejected")
    void initArgumentWithoutRequiredSpecRejected() {
        var store = makeStore();
        for (String omitted : List.of("spec", "dynamic_spec")) {
            var argument = YTree.builder().beginMap()
                    .key("incarnation_id").value(INCARNATION_A.toString())
                    .key("incarnation_generation").value(1);
            if (!omitted.equals("spec")) {
                argument.key("spec").beginMap()
                        .key("parameters").beginMap()
                        .key("companion_resource_class").value("TrackingResource")
                        .endMap()
                        .endMap();
            }
            if (!omitted.equals("dynamic_spec")) {
                argument.key("dynamic_spec").beginMap().endMap();
            }
            var outcome = store.execute(
                    "r", EResourceCommand.RC_INIT_VALUE,
                    YsonUtils.protoFromYTree(argument.endMap().build()));
            assertEquals(EResourceExecuteStatus.RES_ERROR, outcome.status(), omitted);
            assertTrue(outcome.errorMessage().contains(omitted), outcome.errorMessage());
        }
        // Nothing was published: the store must not serve a resource built from a partial argument.
        assertNull(acquiredResource(store, reference("r")));
    }

    @Test
    @DisplayName("An unregistered resource class is reported as not found")
    void unknownClass() {
        var store = makeStore();
        var outcome = execute(store, "r", initArg().setResourceClass("Unknown"));
        assertEquals(EResourceExecuteStatus.RES_RESOURCE_NOT_FOUND, outcome.status());
        assertTrue(outcome.errorMessage().contains("registerResourceClass"));
    }

    @Test
    @DisplayName("An unknown command value is unsupported")
    void unknownCommand() {
        var store = makeStore();
        var outcome = store.execute("r", 100, initArg().build());
        assertEquals(EResourceExecuteStatus.RES_UNSUPPORTED, outcome.status());
    }

    @Test
    @DisplayName("Malformed arguments are reported in-band")
    void malformedArgument() {
        var store = makeStore();
        assertEquals(
                EResourceExecuteStatus.RES_ERROR,
                store.execute("r", EResourceCommand.RC_INIT_VALUE, null).status());
        var listArgument = YsonUtils.protoFromYTree(YTree.builder().beginList().endList().build());
        assertEquals(
                EResourceExecuteStatus.RES_ERROR,
                store.execute("r", EResourceCommand.RC_INIT_VALUE, listArgument).status());
        var emptyMapArgument = YsonUtils.protoFromYTree(YTree.builder().beginMap().endMap().build());
        assertEquals(
                EResourceExecuteStatus.RES_ERROR,
                store.execute("r", EResourceCommand.RC_UNLOAD_VALUE, emptyMapArgument).status());
    }

    @Test
    @DisplayName("A failed load is retryable")
    void failedLoadIsRetryable() {
        var fail = new AtomicBoolean(true);
        var store = makeStore(Map.of("TrackingResource", () -> new TrackingResource(fail.get(), false)));
        assertEquals(EResourceExecuteStatus.RES_ERROR, execute(store, "r", initArg()).status());
        assertNull(acquiredResource(store, reference("r")));
        fail.set(false);
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()).status());
        assertNotNull(acquiredResource(store, reference("r")));
    }

    @Test
    @DisplayName("Dependencies resolve by alias, defaulting to the resource id")
    void dependencyAliasResolution() {
        var store = makeStore();
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "dep", initArg()).status());
        var outcome = execute(store, "r", initArg()
                .setDependencies(List.of(dependencyNode("dep", INCARNATION_A, 0, "my_dep"))));
        assertEquals(EResourceExecuteStatus.RES_OK, outcome.status());
        var dependent = TrackingResource.INSTANCES.get(1);
        assertNotNull(dependent.context);
        assertSame(TrackingResource.INSTANCES.get(0), dependent.context.dependencies().get("my_dep"));

        // Without an alias the dependency is exposed under its resource id.
        outcome = execute(store, "r2", initArg().setDependencies(List.of(dependencyNode("dep"))));
        assertEquals(EResourceExecuteStatus.RES_OK, outcome.status());
        var secondDependent = TrackingResource.INSTANCES.get(2);
        assertNotNull(secondDependent.context);
        assertSame(TrackingResource.INSTANCES.get(0), secondDependent.context.dependencies().get("dep"));
    }

    @Test
    @DisplayName("Missing and stale dependencies are rejected")
    void missingAndStaleDependenciesAreRejected() {
        var store = makeStore();
        var outcome = execute(store, "r", initArg().setDependencies(List.of(dependencyNode("dep"))));
        assertEquals(EResourceExecuteStatus.RES_RESOURCE_NOT_INITIALIZED, outcome.status());
        assertTrue(outcome.errorMessage().contains("dep"));

        // A dependency at a different configuration generation is stale.
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "dep", initArg()).status());
        outcome = execute(store, "r", initArg()
                .setDependencies(List.of(dependencyNode("dep", INCARNATION_A, 5, null))));
        assertEquals(EResourceExecuteStatus.RES_RESOURCE_NOT_INITIALIZED, outcome.status());
    }

    @Test
    @DisplayName("A changed dependency reference recreates the dependent instance")
    void dependencyReferenceChangeRecreatesDependent() {
        var store = makeStore();
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "dep", initArg()).status());
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()
                .setDependencies(List.of(dependencyNode("dep")))).status());

        // Advance the dependency, then re-init the dependent with the new reference.
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "dep", initArg()
                .setConfigurationGeneration(1)
                .setDynamicParameters(Map.of("v", "2"))).status());
        var outcome = execute(store, "r", initArg()
                .setConfigurationGeneration(1)
                .setDependencies(List.of(dependencyNode("dep", INCARNATION_A, 1, null))));
        assertEquals(EResourceExecuteStatus.RES_OK, outcome.status());
        // The dependent was rebuilt against the advanced dependency instance.
        var dependent = TrackingResource.INSTANCES.get(TrackingResource.INSTANCES.size() - 1);
        assertTrue(dependent.loaded);
        assertNotNull(dependent.context);
        assertSame(
                acquiredResource(store, reference("dep", INCARNATION_A, 1, null)),
                dependent.context.dependencies().get("dep"));
    }

    @Test
    @DisplayName("Two concurrent inits of the same resource load once")
    void concurrentInitLoadsOnce() throws InterruptedException {
        var loading = new CountDownLatch(1);
        var proceed = new CountDownLatch(1);

        class BlockingResource extends TrackingResource {
            @Override
            public void load(ResourceContext context) throws ResourceLoadException {
                loading.countDown();
                try {
                    assertTrue(proceed.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new ResourceLoadException("Resource load interrupted", error);
                }
                super.load(context);
            }
        }

        var store = makeStore(Map.of("TrackingResource", BlockingResource::new));
        List<ExecuteOutcome> outcomes = Collections.synchronizedList(new ArrayList<>());
        Runnable run = () -> outcomes.add(execute(store, "r", initArg()));

        var first = new Thread(run);
        var second = new Thread(run);
        first.start();
        assertTrue(loading.await(10, TimeUnit.SECONDS));
        second.start();
        proceed.countDown();
        first.join(TimeUnit.SECONDS.toMillis(10));
        second.join(TimeUnit.SECONDS.toMillis(10));

        assertEquals(2, outcomes.size());
        assertEquals(EResourceExecuteStatus.RES_OK, outcomes.get(0).status());
        assertEquals(EResourceExecuteStatus.RES_OK, outcomes.get(1).status());
        assertEquals(1, TrackingResource.INSTANCES.size());
    }

    @Test
    @DisplayName("Acquire resolves aliases; a stale reference returns null")
    void acquireResolvesAliases() {
        var store = makeStore();
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()).status());
        var lease = store.acquire(List.of(
                reference("r", INCARNATION_A, 0, "view"),
                reference("r")
        ));
        assertNotNull(lease);
        assertEquals(Set.of("view"), lease.resources().keySet());
        lease.release();

        assertNull(store.acquire(List.of(reference("r", INCARNATION_A, 7, null))));
    }

    @Test
    @DisplayName("Unload waits for the batches holding the instance")
    void unloadWaitsForTheBatchesHoldingTheInstance() {
        var store = makeStore();
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()).status());
        var resource = TrackingResource.INSTANCES.get(0);

        var lease = store.acquire(List.of(reference("r", INCARNATION_A, 0, "view")));
        assertNotNull(lease);
        assertSame(resource, lease.resources().get("view"));

        // Retiring the instance bars new acquisitions at once, but the batch still holding it
        // keeps a usable object.
        assertEquals(EResourceExecuteStatus.RES_OK, unload(store, "r", INCARNATION_A).status());
        assertNull(store.acquire(List.of(reference("r", INCARNATION_A, 0, "view"))));
        assertFalse(resource.unloaded);

        lease.release();
        assertTrue(resource.unloaded);
    }

    @Test
    @DisplayName("Replacing an instance waits for the batches holding it")
    void replacingInstanceWaitsForTheBatchesHoldingIt() {
        var store = makeStore();
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()).status());
        var old = TrackingResource.INSTANCES.get(0);
        var lease = store.acquire(List.of(reference("r", INCARNATION_A, 0, "view")));
        assertNotNull(lease);

        // A fresh incarnation replaces the instance under a running batch.
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()
                .setIncarnationId(INCARNATION_B)
                .setIncarnationGeneration(2)).status());
        assertFalse(old.unloaded);
        assertSame(old, lease.resources().get("view"));

        lease.release();
        assertTrue(old.unloaded);
    }

    @Test
    @DisplayName("A dependency outlives its dependent")
    void dependencyOutlivesItsDependent() {
        var store = makeStore();
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "dep", initArg()).status());
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()
                .setDependencies(List.of(dependencyNode("dep")))).status());
        var dependency = TrackingResource.INSTANCES.get(0);
        var dependent = TrackingResource.INSTANCES.get(1);

        // Retiring the dependency must not tear it down while a dependent that captured it is
        // still alive.
        assertEquals(EResourceExecuteStatus.RES_OK, unload(store, "dep", INCARNATION_A).status());
        assertFalse(dependency.unloaded);

        assertEquals(EResourceExecuteStatus.RES_OK, unload(store, "r", INCARNATION_A).status());
        assertTrue(dependent.unloaded);
        assertTrue(dependency.unloaded);
    }

    @Test
    @DisplayName("A dependency is unloaded after its dependent, whatever order the lease releases in")
    void dependencyIsUnloadedAfterItsDependentWhateverTheLeaseOrder() {
        List<String> order = Collections.synchronizedList(new ArrayList<>());

        class OrderedResource extends TrackingResource {
            @Override
            public void unload() {
                order.add(Objects.requireNonNull(context).resourceId());
                super.unload();
            }
        }

        var store = makeStore(Map.of("TrackingResource", OrderedResource::new));
        // "a_pool" sorts before "z_service", and the worker sorts a job's references by resource
        // id, so the lease releases the dependency first.
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "a_pool", initArg()).status());
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "z_service", initArg()
                .setDependencies(List.of(dependencyNode("a_pool")))).status());

        var lease = store.acquire(List.of(
                reference("a_pool", INCARNATION_A, 0, "dep"),
                reference("z_service", INCARNATION_A, 0, "svc")));
        assertNotNull(lease);
        assertEquals(EResourceExecuteStatus.RES_OK, unload(store, "z_service", INCARNATION_A).status());
        assertEquals(EResourceExecuteStatus.RES_OK, unload(store, "a_pool", INCARNATION_A).status());
        assertEquals(List.<String>of(), order);

        lease.release();
        // The dependent must be torn down before the dependency it was built from, however the
        // lease happened to walk its handles.
        assertEquals(List.of("z_service", "a_pool"), order);
    }

    @Test
    @DisplayName("A load failing with an Error still releases the dependency references")
    void failedLoadWithErrorReleasesDependencyReferences() {
        class ErroringResource extends TrackingResource {
            @Override
            public void load(ResourceContext context) {
                throw new NoClassDefFoundError("boom");
            }
        }

        var store = makeStore(Map.of(
                "TrackingResource", TrackingResource::new,
                "ErroringResource", ErroringResource::new));
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "dep", initArg()).status());
        var dependency = TrackingResource.INSTANCES.get(0);

        // An Error is not an in-band failure: it escapes the store the way it always has.
        assertThrows(NoClassDefFoundError.class, () -> execute(store, "r", initArg()
                .setResourceClass("ErroringResource")
                .setDependencies(List.of(dependencyNode("dep")))));
        assertTrue(TrackingResource.INSTANCES.get(1).unloaded);

        // The failed attempt kept no reference on the dependency, so retiring it runs its hook;
        // a leaked reference per attempt would strand it forever.
        assertFalse(dependency.unloaded);
        assertEquals(EResourceExecuteStatus.RES_OK, unload(store, "dep", INCARNATION_A).status());
        assertTrue(dependency.unloaded);
        // The escaping command left the gate on its way out: nothing is left to answer for.
        assertTrue(store.shutdown());
    }

    @Test
    @DisplayName("A failed load whose cleanup hook fails with an Error still releases the dependency references")
    void failedLoadWithErroringCleanupHookReleasesDependencyReferences() {
        class ErroringResource extends TrackingResource {
            @Override
            public void load(ResourceContext context) {
                throw new IllegalStateException("load failed");
            }

            @Override
            public void unload() {
                super.unload();
                throw new NoClassDefFoundError("boom");
            }
        }

        var store = makeStore(Map.of(
                "TrackingResource", TrackingResource::new,
                "ErroringResource", ErroringResource::new));
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "dep", initArg()).status());
        var dependency = TrackingResource.INSTANCES.get(0);

        // The cleanup hook of the half-built instance escapes with an Error, which is not swallowed.
        assertThrows(NoClassDefFoundError.class, () -> execute(store, "r", initArg()
                .setResourceClass("ErroringResource")
                .setDependencies(List.of(dependencyNode("dep")))));
        assertTrue(TrackingResource.INSTANCES.get(1).unloaded);

        // No handle owns the dependency references yet, so the escaping hook must not skip their
        // release: the worker retries init forever and one leak per attempt would strand the
        // dependency's own hook.
        assertFalse(dependency.unloaded);
        assertEquals(EResourceExecuteStatus.RES_OK, unload(store, "dep", INCARNATION_A).status());
        assertTrue(dependency.unloaded);
    }

    @Test
    @DisplayName("A factory that throws releases the dependency references it acquired")
    void failingFactoryReleasesAcquiredDependencies() {
        var fail = new AtomicBoolean(false);
        var store = makeStore(Map.of("TrackingResource", () -> {
            if (fail.get()) {
                throw new IllegalStateException("factory failed");
            }
            return new TrackingResource();
        }));
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "dep", initArg()).status());
        var dependency = TrackingResource.INSTANCES.get(0);

        // The dependencies are acquired before the instance is built, so a factory that throws must
        // not strand those references: the worker retries the failed init forever.
        fail.set(true);
        for (int attempt = 0; attempt < 3; attempt++) {
            var outcome = execute(store, "r", initArg().setDependencies(List.of(dependencyNode("dep"))));
            assertEquals(EResourceExecuteStatus.RES_ERROR, outcome.status());
        }

        // One reference leaked per attempt would keep the dependency's refcount above zero forever.
        assertEquals(EResourceExecuteStatus.RES_OK, unload(store, "dep", INCARNATION_A).status());
        assertTrue(dependency.unloaded);
    }

    @Test
    @DisplayName("An unload hook failing with an Error still releases the dependency references")
    void unloadHookFailingWithErrorReleasesDependencyReferences() {
        class ErroringResource extends TrackingResource {
            @Override
            public void unload() {
                super.unload();
                throw new NoClassDefFoundError("boom");
            }
        }

        var store = makeStore(Map.of(
                "TrackingResource", TrackingResource::new,
                "ErroringResource", ErroringResource::new));
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "dep", initArg()).status());
        var dependency = TrackingResource.INSTANCES.get(0);
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()
                .setResourceClass("ErroringResource")
                .setDependencies(List.of(dependencyNode("dep")))).status());

        // The hook is user code: an Error is not swallowed, it escapes the store.
        assertThrows(NoClassDefFoundError.class, () -> unload(store, "r", INCARNATION_A));

        // The dependent is retired either way, so its references on the dependencies must have been
        // dropped: they can never be recovered once a repeated unload returns success.
        assertFalse(dependency.unloaded);
        assertEquals(EResourceExecuteStatus.RES_OK, unload(store, "dep", INCARNATION_A).status());
        assertTrue(dependency.unloaded);
    }

    @Test
    @DisplayName("Unload releases the retired instance's context")
    void unloadReleasesTheRetiredContext() {
        var contexts = new ArrayList<WeakReference<ResourceContext>>();
        var store = makeStore(Map.of(
                "TrackingResource", TrackingResource::new,
                "ContextOblivious", () -> new ContextObliviousResource(contexts)));
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "dep", initArg()).status());
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()
                .setResourceClass("ContextOblivious")
                .setDependencies(List.of(dependencyNode("dep")))).status());
        assertEquals(1, contexts.size());

        assertEquals(EResourceExecuteStatus.RES_OK, unload(store, "r", INCARNATION_A).status());

        // The entry outlives the instance, so a retained context would keep the retired
        // dependency instances and parsed parameters reachable for the process's life.
        assertUnreachable(contexts.get(0), "the retired resource context");
    }

    @Test
    @DisplayName("A failed replacement releases the retired instance's context")
    void failedReplacementReleasesTheRetiredContext() {
        var contexts = new ArrayList<WeakReference<ResourceContext>>();
        var store = makeStore(Map.of(
                "TrackingResource", TrackingResource::new,
                "ContextOblivious", () -> new ContextObliviousResource(contexts)));
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "dep", initArg()).status());
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()
                .setResourceClass("ContextOblivious")
                .setDependencies(List.of(dependencyNode("dep")))).status());
        assertEquals(1, contexts.size());

        // Same incarnation, changed dependency references: the entry detaches its instance and then
        // fails on the unavailable dependency, so it must keep nothing of it.
        assertEquals(
                EResourceExecuteStatus.RES_RESOURCE_NOT_INITIALIZED,
                execute(store, "r", initArg()
                        .setResourceClass("ContextOblivious")
                        .setDependencies(List.of(dependencyNode("absent")))).status());

        assertUnreachable(contexts.get(0), "the retired resource context");
    }

    @Test
    @DisplayName("A failed reconfigure releases the quarantined instance's context")
    void failedReconfigureReleasesTheQuarantinedContext() {
        var contexts = new ArrayList<WeakReference<ResourceContext>>();
        var store = makeStore(Map.of(
                "TrackingResource", TrackingResource::new,
                "FailingReconfigure", () -> new ContextObliviousResource(contexts, true)));
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "dep", initArg()).status());
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()
                .setResourceClass("FailingReconfigure")
                .setDependencies(List.of(dependencyNode("dep")))).status());
        assertEquals(1, contexts.size());

        assertEquals(
                EResourceExecuteStatus.RES_ERROR,
                execute(store, "r", initArg()
                        .setResourceClass("FailingReconfigure")
                        .setConfigurationGeneration(1)
                        .setDynamicParameters(Map.of("v", "2"))
                        .setDependencies(List.of(dependencyNode("dep")))).status());

        // The failed hook detached the instance, so the entry serves nothing and must not keep the
        // quarantined generation's dependencies and parameters reachable.
        assertUnreachable(contexts.get(0), "the quarantined resource context");
    }

    @Test
    @DisplayName("Shutdown unloads every hosted resource and bars further acquisition")
    void shutdownUnloadsEveryHostedResource() {
        var store = makeStore();
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "dep", initArg()).status());
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()
                .setDependencies(List.of(dependencyNode("dep")))).status());
        var dependency = TrackingResource.INSTANCES.get(0);
        var dependent = TrackingResource.INSTANCES.get(1);

        assertTrue(store.shutdown());
        assertTrue(dependent.unloaded);
        assertTrue(dependency.unloaded);
        assertNull(acquiredResource(store, reference("r")));
        assertNull(store.acquire(List.of(reference("dep"))));

        // Idempotent: a second shutdown finds nothing left to release.
        assertTrue(store.shutdown());
    }

    @Test
    @DisplayName("Shutdown defers to the batch still holding a lease")
    void shutdownDefersToTheBatchStillHoldingALease() {
        var store = makeStore();
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()).status());
        var resource = TrackingResource.INSTANCES.get(0);
        var lease = store.acquire(List.of(reference("r", INCARNATION_A, 0, "view")));
        assertNotNull(lease);

        // Reports the lease still out: the hook will run at the batch's release, not here.
        assertFalse(store.shutdown());
        assertFalse(resource.unloaded);
        assertSame(resource, lease.resources().get("view"));

        // A second shutdown must not drop the store's reference again: that would tear the instance
        // down under the batch still using it. It still answers for the lease, though: true would
        // promise that no hook of this store runs later.
        assertFalse(store.shutdown());
        assertFalse(resource.unloaded);

        lease.release();
        assertTrue(resource.unloaded);
        assertTrue(store.shutdown());
    }

    @Test
    @DisplayName("Shutdown answers for an instance retired before it that a lease still holds")
    void shutdownAnswersForTheRetiredInstanceALeaseStillHolds() {
        var store = makeStore();
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()).status());
        var resource = TrackingResource.INSTANCES.get(0);
        var lease = store.acquire(List.of(reference("r")));
        assertNotNull(lease);

        // The entry no longer publishes the instance, but the lease keeps its unload hook pending.
        assertEquals(EResourceExecuteStatus.RES_OK, unload(store, "r", INCARNATION_A).status());
        assertFalse(resource.unloaded);

        // Nothing is published to detach, yet the hook is still to run on the batch's release.
        assertFalse(store.shutdown());
        assertFalse(resource.unloaded);

        lease.release();
        assertTrue(resource.unloaded);
        assertTrue(store.shutdown());
    }

    @Test
    @DisplayName("An init finishing after shutdown does not strand its instance")
    void initFinishingAfterShutdownDoesNotStrandTheInstance() {
        var storeRef = new AtomicReference<ResourceStore>();
        var drained = new AtomicBoolean();

        class LateResource extends TrackingResource {
            @Override
            public void load(ResourceContext context) throws ResourceLoadException {
                // Stands in for a command still running user code when the serving generation is
                // torn down. Re-entered on the command's own thread, the drain does not wait on
                // the lifecycle lock; it must still answer for this load's pending unload hook.
                assertFalse(storeRef.get().shutdown());
                drained.set(true);
                super.load(context);
            }
        }

        var store = makeStore(Map.of("TrackingResource", LateResource::new));
        storeRef.set(store);
        var outcome = execute(store, "r", initArg());

        assertTrue(drained.get());
        assertEquals(EResourceExecuteStatus.RES_ERROR, outcome.status());
        // The entry is unreachable by then, so publishing would strand the instance: it must be
        // torn down instead.
        var resource = TrackingResource.INSTANCES.get(0);
        assertEquals(1, resource.unloadCount.get());
        assertNull(store.acquire(List.of(reference("r"))));
        assertTrue(store.shutdown());
        assertEquals(1, resource.unloadCount.get());
    }

    @Test
    @DisplayName("Commands are refused once the store is shut down")
    void commandsAreRefusedOnceTheStoreIsShutDown() {
        var store = makeStore();
        store.shutdown();

        // A late command must not repopulate the store it was just drained from.
        assertEquals(EResourceExecuteStatus.RES_ERROR, execute(store, "r", initArg()).status());
        assertTrue(TrackingResource.INSTANCES.isEmpty());
        assertNull(store.acquire(List.of(reference("r"))));
    }

    @Test
    @DisplayName("A reconfigure finishing after shutdown is not republished")
    void reconfigureFinishingAfterShutdownIsNotRepublished() {
        var storeRef = new AtomicReference<ResourceStore>();

        class LateReconfigureResource extends TrackingResource {
            @Override
            public void reconfigure(ResourceContext context) throws Exception {
                // Stands in for a hook still running when the serving generation is torn down.
                // Re-entered on the command's own thread, the drain cannot wait for this hook: it
                // must answer for it, and it must not unload the instance under it.
                assertFalse(storeRef.get().shutdown());
                assertFalse(unloaded);
                super.reconfigure(context);
            }
        }

        var store = makeStore(Map.of("TrackingResource", LateReconfigureResource::new));
        storeRef.set(store);
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()).status());
        var resource = TrackingResource.INSTANCES.get(0);

        var outcome = execute(store, "r", initArg()
                .setDynamicParameters(Map.of("v", "2"))
                .setConfigurationGeneration(1));

        assertEquals(1, resource.reconfigureCount);
        // The unload the drain triggered ran once the hook had returned.
        assertTrue(resource.unloaded);
        // Nothing to publish afterwards: the entry is unreachable.
        assertEquals(EResourceExecuteStatus.RES_ERROR, outcome.status());
        assertNull(store.acquire(List.of(reference("r", INCARNATION_A, 1, "view"))));
        assertTrue(store.shutdown());
    }

    @Test
    @DisplayName("A command re-entered from a hook is refused and the outer command completes")
    void commandReenteredFromAHookIsRefused() {
        var storeRef = new AtomicReference<ResourceStore>();
        var nested = new AtomicReference<ExecuteOutcome>();

        class ReenteringResource extends TrackingResource {
            @Override
            public void reconfigure(ResourceContext context) throws Exception {
                // A hook unloading the very instance it is reconfiguring: admitted by the
                // reentrant lock, the outer command would republish an unloaded handle.
                nested.set(ResourceStoreTest.unload(storeRef.get(), "r", INCARNATION_A));
                super.reconfigure(context);
            }
        }

        var store = makeStore(Map.of("TrackingResource", ReenteringResource::new));
        storeRef.set(store);
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()).status());
        var resource = TrackingResource.INSTANCES.get(0);

        var outcome = execute(store, "r", initArg()
                .setDynamicParameters(Map.of("v", "2"))
                .setConfigurationGeneration(1));

        assertEquals(EResourceExecuteStatus.RES_ERROR, nested.get().status());
        assertTrue(nested.get().errorMessage().contains("re-entered"));
        assertEquals(EResourceExecuteStatus.RES_OK, outcome.status());
        assertFalse(resource.unloaded);
        assertSame(resource, acquiredResource(store, reference("r", INCARNATION_A, 1, null)));
    }

    @Test
    @DisplayName("Shutdown never waits for an admitted load and still accounts for its cleanup")
    void shutdownDoesNotWaitForAnAdmittedLoad() throws Exception {
        var loadEntered = new CountDownLatch(1);
        var loadMayFinish = new CountDownLatch(1);
        class BlockingResource extends TrackingResource {
            @Override
            public void load(ResourceContext context) throws ResourceLoadException {
                loadEntered.countDown();
                try {
                    assertTrue(loadMayFinish.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new ResourceLoadException("Resource load interrupted", error);
                }
                super.load(context);
            }
        }
        var store = makeStore(Map.of("TrackingResource", BlockingResource::new));
        var executor = Executors.newFixedThreadPool(3);
        try {
            var init = executor.submit(() -> execute(store, "r", initArg()));
            assertTrue(loadEntered.await(5, TimeUnit.SECONDS));
            var queuedInit = executor.submit(() -> execute(store, "r", initArg()));
            assertFalse(executor.submit(store::shutdown).get(2, TimeUnit.SECONDS));
            assertFalse(store.shutdown());
            assertNull(store.acquire(List.of()));
            loadMayFinish.countDown();
            assertEquals(EResourceExecuteStatus.RES_ERROR, init.get(5, TimeUnit.SECONDS).status());
            assertEquals(EResourceExecuteStatus.RES_ERROR, queuedInit.get(5, TimeUnit.SECONDS).status());
            assertEquals(1, TrackingResource.INSTANCES.size());
            assertTrue(TrackingResource.INSTANCES.get(0).unloaded);
            assertTrue(store.shutdown());
        } finally {
            loadMayFinish.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    @DisplayName("A lease releases every handle even when a hook escapes")
    void leaseReleasesEveryHandleEvenWhenAHookEscapes() {
        class ExplodingResource extends TrackingResource {
            @Override
            public void unload() {
                super.unload();
                // Only an Error escapes the hook runner; a RuntimeException is logged there.
                throw new NoClassDefFoundError("hook escaped");
            }
        }

        var store = makeStore(Map.of("TrackingResource", ExplodingResource::new));
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "a", initArg()).status());
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "b", initArg()).status());
        var lease = store.acquire(List.of(
                reference("a", INCARNATION_A, 0, "a"),
                reference("b", INCARNATION_A, 0, "b")
        ));
        assertNotNull(lease);

        // The lease still holds both instances, so the drain defers to it.
        store.shutdown();
        assertFalse(TrackingResource.INSTANCES.get(0).unloaded);

        assertThrows(NoClassDefFoundError.class, lease::release);

        // The first escaping hook must not cost the other resource its teardown.
        assertTrue(TrackingResource.INSTANCES.stream().allMatch(instance -> instance.unloaded));
    }

    @Test
    @DisplayName("Shutdown drains every entry even when a hook escapes")
    void shutdownDrainsEveryEntryEvenWhenAHookEscapes() {
        class ExplodingResource extends TrackingResource {
            @Override
            public void unload() {
                super.unload();
                throw new NoClassDefFoundError("hook escaped");
            }
        }

        var store = makeStore(Map.of("TrackingResource", ExplodingResource::new));
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "a", initArg()).status());
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "b", initArg()).status());

        var escaped = assertThrows(NoClassDefFoundError.class, store::shutdown);
        assertEquals("hook escaped", escaped.getMessage());

        // The first escaping hook must not cost the remaining entries theirs: they are already out
        // of the store, so nothing could ever reach them again.
        assertEquals(2, TrackingResource.INSTANCES.size());
        for (var instance : TrackingResource.INSTANCES) {
            assertTrue(instance.unloaded);
        }
    }

    @Test
    @DisplayName("A failed load unloads the half-built instance")
    void failedLoadUnloadsTheHalfBuiltInstance() {
        var store = makeStore(Map.of("TrackingResource", () -> new TrackingResource(true, false)));
        assertEquals(EResourceExecuteStatus.RES_ERROR, execute(store, "r", initArg()).status());
        // The worker retries init forever, so anything the failed load opened has to be released
        // instead of piling up per attempt.
        var resource = TrackingResource.INSTANCES.get(0);
        assertEquals(1, resource.unloadCount.get());
        assertTrue(store.shutdown());
        assertEquals(1, resource.unloadCount.get());
    }

    @Test
    void typedLoadFailureIsRetryable() {
        var attempts = new AtomicInteger();
        class RetriableResource extends TrackingResource {
            @Override
            public void load(ResourceContext context) throws ResourceLoadException {
                super.load(context);
                if (attempts.getAndIncrement() == 0) {
                    throw new ResourceLoadException("Pool initialization failed");
                }
            }
        }
        var store = makeStore(Map.of("TrackingResource", RetriableResource::new));
        try {
            var failed = execute(store, "r", initArg());
            assertEquals(EResourceExecuteStatus.RES_ERROR, failed.status());
            assertTrue(failed.errorMessage().contains("Pool initialization failed"));
            assertNull(acquiredResource(store, reference("r")));
            var partial = TrackingResource.INSTANCES.get(0);
            assertEquals(1, partial.unloadCount.get());

            assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()).status());
            var loaded = TrackingResource.INSTANCES.get(1);
            assertSame(loaded, acquiredResource(store, reference("r")));
            assertNotSame(partial, loaded);
            assertEquals(2, attempts.get());
            assertEquals(0, loaded.unloadCount.get());
            assertTrue(store.shutdown());
            assertEquals(1, partial.unloadCount.get());
            assertEquals(1, loaded.unloadCount.get());
        } finally {
            store.shutdown();
        }
    }

    @Test
    void failedLoadRollbackUnloadsBeforeReleasingDependencies() {
        var storeRef = new AtomicReference<ResourceStore>();
        var order = new ArrayList<String>();
        class Dependency extends TrackingResource {
            @Override
            public void unload() {
                order.add("dependency");
                super.unload();
            }
        }
        class FailingResource extends TrackingResource {
            @Override
            public void load(ResourceContext context) throws ResourceLoadException {
                super.load(context);
                // Leave the loading resource as the dependency's only owner.
                assertEquals(EResourceExecuteStatus.RES_OK,
                        ResourceStoreTest.unload(storeRef.get(), "dep", INCARNATION_A).status());
                throw new ResourceLoadException("load failed after acquiring the dependency");
            }

            @Override
            public void unload() {
                assertNotNull(context);
                var dependency = context.dependency("dep", Dependency.class);
                assertFalse(dependency.unloaded);
                order.add("dependent");
                super.unload();
            }
        }
        var store = makeStore(Map.of("Dependency", Dependency::new, "FailingResource", FailingResource::new));
        storeRef.set(store);
        try {
            assertEquals(EResourceExecuteStatus.RES_OK,
                    execute(store, "dep", initArg().setResourceClass("Dependency")).status());
            var outcome = execute(store, "r", initArg().setResourceClass("FailingResource")
                    .setDependencies(List.of(dependencyNode("dep"))));
            assertEquals(EResourceExecuteStatus.RES_ERROR, outcome.status());
            assertEquals(List.of("dependent", "dependency"), order);
            assertNull(acquiredResource(store, reference("r")));
            assertTrue(store.shutdown());
            for (var instance : TrackingResource.INSTANCES) {
                assertEquals(1, instance.unloadCount.get());
            }
        } finally {
            store.shutdown();
        }
    }

    @Test
    void failedLoadCleanupKeepsStoreNonQuiescentUntilUnloadReturns() throws Exception {
        var entered = new CountDownLatch(1);
        var proceed = new CountDownLatch(1);
        var partial = new TrackingResource(true, false) {
            @Override
            public void unload() {
                entered.countDown();
                try {
                    assertTrue(proceed.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(error);
                }
                super.unload();
            }
        };
        var store = makeStore(Map.of("TrackingResource", () -> partial));
        var executor = Executors.newFixedThreadPool(2);
        try {
            var loading = executor.submit(() -> execute(store, "r", initArg()));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertNull(store.acquire(List.of(reference("r"))));
            assertFalse(executor.submit(store::shutdown).get(2, TimeUnit.SECONDS));
            assertEquals(0, partial.unloadCount.get());
            proceed.countDown();
            assertEquals(EResourceExecuteStatus.RES_ERROR, loading.get(5, TimeUnit.SECONDS).status());
            assertTrue(store.shutdown());
            assertEquals(1, partial.unloadCount.get());
        } finally {
            proceed.countDown();
            executor.shutdownNow();
            store.shutdown();
        }
    }

    @Test
    void nullFactoryResultReleasesDependencies() {
        var store = makeStore(Map.of("TrackingResource", TrackingResource::new, "NullResource", () -> null));
        try {
            assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "dep", initArg()).status());
            var dependency = TrackingResource.INSTANCES.get(0);
            for (int attempt = 0; attempt < 3; ++attempt) {
                var result = execute(store, "r", initArg().setResourceClass("NullResource")
                        .setDependencies(List.of(dependencyNode("dep"))));
                assertEquals(EResourceExecuteStatus.RES_ERROR, result.status());
                assertTrue(result.errorMessage().contains("Resource factory returned null"));
                assertNull(acquiredResource(store, reference("r")));
            }
            assertEquals(0, dependency.unloadCount.get());
            assertEquals(EResourceExecuteStatus.RES_OK, unload(store, "dep", INCARNATION_A).status());
            assertEquals(1, dependency.unloadCount.get());
            assertTrue(store.shutdown());
        } finally {
            store.shutdown();
        }
    }

    @Test
    void failedLoadRollbackPromotesFatalDependencyCleanup() {
        var storeRef = new AtomicReference<ResourceStore>();
        var loadFailure = new ResourceLoadException("load failed");
        var unloadFailure = new LinkageError("dependent cleanup failed");
        var fatal = new OutOfMemoryError("dependency cleanup failed, simulated");
        class Dependency extends TrackingResource {
            @Override
            public void unload() {
                super.unload();
                throw fatal;
            }
        }
        class FailingResource extends TrackingResource {
            @Override
            public void load(ResourceContext context) throws ResourceLoadException {
                assertEquals(EResourceExecuteStatus.RES_OK,
                        ResourceStoreTest.unload(storeRef.get(), "dep", INCARNATION_A).status());
                throw loadFailure;
            }

            @Override
            public void unload() {
                super.unload();
                throw unloadFailure;
            }
        }
        var store = makeStore(Map.of("Dependency", Dependency::new, "FailingResource", FailingResource::new));
        storeRef.set(store);
        assertEquals(EResourceExecuteStatus.RES_OK,
                execute(store, "dep", initArg().setResourceClass("Dependency")).status());
        var escaped = assertThrows(OutOfMemoryError.class, () -> execute(store, "r",
                initArg().setResourceClass("FailingResource").setDependencies(List.of(dependencyNode("dep")))));
        assertSame(fatal, escaped);
        assertArrayEquals(new Throwable[]{unloadFailure}, fatal.getSuppressed());
        assertArrayEquals(new Throwable[]{loadFailure}, unloadFailure.getSuppressed());
        assertTrue(store.shutdown());
        for (var instance : TrackingResource.INSTANCES) {
            assertEquals(1, instance.unloadCount.get());
        }
    }

    @Test
    @DisplayName("Reconfigure bars acquisitions while it runs")
    void reconfigureBarsAcquisitionsWhileItRuns() {
        var storeRef = new AtomicReference<ResourceStore>();
        var reconfigured = new AtomicBoolean();
        var observedDuringReconfigure = new AtomicReference<ResourceLease>();

        class ObservingResource extends TrackingResource {
            @Override
            public void reconfigure(ResourceContext context) throws Exception {
                reconfigured.set(true);
                // The instance is mid-reconfigure: neither the old nor the new generation may be
                // handed to a batch.
                observedDuringReconfigure.set(
                        storeRef.get().acquire(List.of(reference("r", INCARNATION_A, 0, null))));
                super.reconfigure(context);
            }
        }

        var store = makeStore(Map.of("TrackingResource", ObservingResource::new));
        storeRef.set(store);
        assertEquals(EResourceExecuteStatus.RES_OK, execute(store, "r", initArg()).status());
        var outcome = execute(store, "r", initArg()
                .setConfigurationGeneration(1)
                .setDynamicParameters(Map.of("v", "2")));

        assertEquals(EResourceExecuteStatus.RES_OK, outcome.status());
        assertTrue(reconfigured.get());
        // No batch may start against half-applied parameters.
        assertNull(observedDuringReconfigure.get());
        var lease = store.acquire(List.of(reference("r", INCARNATION_A, 1, null)));
        assertNotNull(lease);
        lease.release();
    }

    @Test
    @DisplayName("An in-band error never carries an empty message")
    void errorMessageIsNeverEmpty() {
        class SilentlyFailingResource extends TrackingResource {
            @Override
            public void load(ResourceContext context) {
                throw new IllegalStateException();
            }
        }

        var store = makeStore(Map.of("TrackingResource", SilentlyFailingResource::new));
        var outcome = execute(store, "r", initArg());
        assertEquals(EResourceExecuteStatus.RES_ERROR, outcome.status());
        // An empty message would reach the worker as a resource failure with no cause at all.
        assertFalse(outcome.errorMessage().isEmpty());
    }

    @Test
    void shutdownDoesNotUnloadAnInFlightReconfigure() throws Exception {
        var entered = new CountDownLatch(1);
        var proceed = new CountDownLatch(1);
        class BlockingReconfigure extends TrackingResource {
            @Override
            public void reconfigure(ResourceContext context) throws Exception {
                entered.countDown();
                assertTrue(proceed.await(10, TimeUnit.SECONDS));
                assertFalse(unloaded);
                super.reconfigure(context);
            }
        }
        var store = makeStore(Map.of("TrackingResource", BlockingReconfigure::new));
        execute(store, "r", initArg());
        var resource = TrackingResource.INSTANCES.get(0);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var reconfigure = executor.submit(() -> execute(store, "r", initArg().setConfigurationGeneration(1)));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertFalse(executor.submit(store::shutdown).get(2, TimeUnit.SECONDS));
            assertFalse(resource.unloaded);
            assertNull(store.acquire(List.of(reference("r"))));
            proceed.countDown();
            assertEquals(EResourceExecuteStatus.RES_ERROR, reconfigure.get(5, TimeUnit.SECONDS).status());
            assertTrue(resource.unloaded);
            assertTrue(store.shutdown());
        } finally {
            proceed.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void concurrentShutdownAccountsForBlockedUnloadHook() throws Exception {
        var entered = new CountDownLatch(1);
        var proceed = new CountDownLatch(1);
        class BlockingUnload extends TrackingResource {
            @Override
            public void unload() {
                entered.countDown();
                try {
                    assertTrue(proceed.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                super.unload();
            }
        }
        var store = makeStore(Map.of("TrackingResource", BlockingUnload::new));
        execute(store, "r", initArg());
        var executor = Executors.newFixedThreadPool(2);
        try {
            var firstShutdown = executor.submit(store::shutdown);
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertFalse(executor.submit(store::shutdown).get(2, TimeUnit.SECONDS));
            proceed.countDown();
            assertTrue(firstShutdown.get(5, TimeUnit.SECONDS));
            assertTrue(store.shutdown());
        } finally {
            proceed.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void unrelatedAcquisitionDoesNotWaitForLoadHook() throws Exception {
        var entered = new CountDownLatch(1);
        var proceed = new CountDownLatch(1);
        class BlockingLoad extends TrackingResource {
            @Override
            public void load(ResourceContext context) throws ResourceLoadException {
                if (context.resourceId().equals("slow")) {
                    entered.countDown();
                    try {
                        assertTrue(proceed.await(10, TimeUnit.SECONDS));
                    } catch (InterruptedException error) {
                        Thread.currentThread().interrupt();
                        throw new ResourceLoadException("Resource load interrupted", error);
                    }
                }
                super.load(context);
            }
        }
        var store = makeStore(Map.of("TrackingResource", BlockingLoad::new));
        execute(store, "ready", initArg());
        var executor = Executors.newFixedThreadPool(2);
        try {
            var loading = executor.submit(() -> execute(store, "slow", initArg()));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            var lease = executor.submit(() -> store.acquire(List.of(reference("ready"))))
                    .get(2, TimeUnit.SECONDS);
            assertNotNull(lease);
            lease.close();
            proceed.countDown();
            assertEquals(EResourceExecuteStatus.RES_OK, loading.get(5, TimeUnit.SECONDS).status());
        } finally {
            proceed.countDown();
            executor.shutdownNow();
            store.shutdown();
        }
    }

    @Test
    void rejectedAcquisitionDoesNotRetainItsValidPrefix() {
        var store = makeStore();
        execute(store, "r", initArg());
        assertNull(store.acquire(List.of(reference("r"), reference("missing"))));
        unload(store, "r", INCARNATION_A);
        assertTrue(TrackingResource.INSTANCES.get(0).unloaded);
        assertTrue(store.shutdown());
    }

    @Test
    void reconfigureErrorQuarantinesAndReleasesTheInstance() {
        var failure = new LinkageError("reconfigure failed");
        class ErroringReconfigure extends TrackingResource {
            @Override
            public void reconfigure(ResourceContext context) {
                throw failure;
            }
        }
        var store = makeStore(Map.of("TrackingResource", ErroringReconfigure::new));
        execute(store, "r", initArg());
        assertSame(failure, assertThrows(LinkageError.class,
                () -> execute(store, "r", initArg().setConfigurationGeneration(1))));
        assertTrue(TrackingResource.INSTANCES.get(0).unloaded);
        assertNull(store.acquire(List.of(reference("r"))));
        assertEquals(EResourceExecuteStatus.RES_OK,
                execute(store, "r", initArg().setConfigurationGeneration(1)).status());
        assertEquals(2, TrackingResource.INSTANCES.size());
        assertTrue(store.shutdown());
    }

    @Test
    void leaseAndShutdownPromoteALaterFatalUnloadError() {
        for (boolean leased : List.of(false, true)) {
            var calls = new AtomicInteger();
            var ordinary = new LinkageError("first");
            var fatal = new OutOfMemoryError("second, simulated");
            class FailingUnload extends TrackingResource {
                @Override
                public void unload() {
                    super.unload();
                    if (calls.getAndIncrement() == 0) {
                        throw ordinary;
                    }
                    throw fatal;
                }
            }
            var store = makeStore(Map.of("TrackingResource", FailingUnload::new));
            execute(store, "a", initArg());
            execute(store, "b", initArg());
            if (leased) {
                var lease = store.acquire(List.of(reference("a"), reference("b")));
                assertNotNull(lease);
                assertFalse(store.shutdown());
                assertSame(fatal, assertThrows(OutOfMemoryError.class, lease::close));
            } else {
                assertSame(fatal, assertThrows(OutOfMemoryError.class, store::shutdown));
            }
            assertEquals(2, calls.get());
            assertSame(ordinary, fatal.getSuppressed()[0]);
            assertTrue(store.shutdown());
        }
    }

    @Test
    void fatalDependencyCleanupTakesPrecedenceOverDependentUnloadError() {
        var fatal = new OutOfMemoryError("dependency, simulated");
        var ordinary = new LinkageError("dependent");
        class FailingUnload extends TrackingResource {
            @Override
            public void unload() {
                super.unload();
                if (context.resourceId().equals("dep")) {
                    throw fatal;
                }
                throw ordinary;
            }
        }
        var store = makeStore(Map.of("TrackingResource", FailingUnload::new));
        execute(store, "dep", initArg());
        execute(store, "r", initArg().setDependencies(List.of(dependencyNode("dep"))));
        unload(store, "dep", INCARNATION_A);
        assertSame(fatal, assertThrows(OutOfMemoryError.class, () -> unload(store, "r", INCARNATION_A)));
        assertSame(ordinary, fatal.getSuppressed()[0]);
        assertTrue(store.shutdown());
    }

    @Test
    void concurrentLeaseCloseDropsReferencesExactlyOnce() throws Exception {
        var calls = new AtomicInteger();
        class CountedUnload extends TrackingResource {
            @Override
            public void unload() {
                calls.incrementAndGet();
                super.unload();
            }
        }
        var store = makeStore(Map.of("TrackingResource", CountedUnload::new));
        execute(store, "r", initArg());
        var lease = store.acquire(List.of(reference("r"), reference("r")));
        assertNotNull(lease);
        assertFalse(store.shutdown());
        var executor = Executors.newFixedThreadPool(2);
        var start = new CountDownLatch(1);
        try {
            var first = executor.submit(() -> {
                start.await();
                lease.close();
                return true;
            });
            var second = executor.submit(() -> {
                start.await();
                lease.close();
                return true;
            });
            start.countDown();
            assertTrue(first.get(5, TimeUnit.SECONDS));
            assertTrue(second.get(5, TimeUnit.SECONDS));
            assertEquals(1, calls.get());
            assertTrue(store.shutdown());
        } finally {
            start.countDown();
            executor.shutdownNow();
        }
    }

    /**
     * Fails unless the referent becomes collectable, which is what "the store keeps nothing of the
     * retired generation" means: the field being cleared is only the mechanism.
     */
    private static void assertUnreachable(WeakReference<?> reference, String what) {
        for (int attempt = 0; attempt < 50; ++attempt) {
            if (reference.get() == null) {
                return;
            }
            System.gc();
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        fail("%s is still reachable from the store".formatted(what));
    }

    private static ExecuteOutcome execute(ResourceStore store, String resourceId, InitArg argument) {
        return store.execute(resourceId, EResourceCommand.RC_INIT_VALUE, argument.build());
    }

    private static ExecuteOutcome unload(ResourceStore store, String resourceId, GUID incarnationId) {
        return store.execute(resourceId, EResourceCommand.RC_UNLOAD_VALUE, unloadArg(incarnationId));
    }

    /**
     * Resource that records only a weak reference to the context it was loaded with, so what keeps
     * that context alive afterwards is the store and nothing else.
     */
    static final class ContextObliviousResource implements FlowResource {
        private final List<WeakReference<ResourceContext>> contexts;
        private final boolean failReconfigure;

        ContextObliviousResource(List<WeakReference<ResourceContext>> contexts) {
            this(contexts, false);
        }

        ContextObliviousResource(List<WeakReference<ResourceContext>> contexts, boolean failReconfigure) {
            this.contexts = contexts;
            this.failReconfigure = failReconfigure;
        }

        @Override
        public void load(ResourceContext context) {
            contexts.add(new WeakReference<>(context));
        }

        @Override
        public void unload() {
            // Only weak references are recorded; there are no external resources to release.
        }

        @Override
        public void reconfigure(ResourceContext context) throws Exception {
            if (failReconfigure) {
                throw new RuntimeException("reconfigure failed");
            }
        }
    }

    /**
     * Records lifecycle calls and current parameters.
     */
    static class TrackingResource implements FlowResource {
        static final List<TrackingResource> INSTANCES = Collections.synchronizedList(new ArrayList<>());

        final boolean failLoad;
        final boolean failReconfigure;
        volatile boolean loaded;
        volatile boolean unloaded;
        final AtomicInteger unloadCount = new AtomicInteger();
        volatile int reconfigureCount;
        volatile @Nullable ResourceContext context;

        TrackingResource() {
            this(false, false);
        }

        TrackingResource(boolean failLoad, boolean failReconfigure) {
            this.failLoad = failLoad;
            this.failReconfigure = failReconfigure;
            INSTANCES.add(this);
        }

        @Override
        public void load(ResourceContext context) throws ResourceLoadException {
            if (failLoad) {
                throw new ResourceLoadException("load failed");
            }
            loaded = true;
            this.context = context;
        }

        @Override
        public void reconfigure(ResourceContext context) throws Exception {
            if (failReconfigure) {
                throw new RuntimeException("reconfigure failed");
            }
            reconfigureCount++;
            this.context = context;
        }

        @Override
        public void unload() {
            unloadCount.incrementAndGet();
            unloaded = true;
        }
    }

    /**
     * Fluent builder of the init command YSON argument.
     */
    private static final class InitArg {
        private GUID incarnationId = INCARNATION_A;
        private long incarnationGeneration = 1;
        private long configurationGeneration = 0;
        private Map<String, String> parameters = Map.of();
        private Map<String, String> dynamicParameters = Map.of();
        private List<YTreeNode> dependencies = List.of();
        private String resourceClass = "TrackingResource";

        InitArg setIncarnationId(GUID incarnationId) {
            this.incarnationId = incarnationId;
            return this;
        }

        InitArg setIncarnationGeneration(long incarnationGeneration) {
            this.incarnationGeneration = incarnationGeneration;
            return this;
        }

        InitArg setConfigurationGeneration(long configurationGeneration) {
            this.configurationGeneration = configurationGeneration;
            return this;
        }

        InitArg setParameters(Map<String, String> parameters) {
            this.parameters = parameters;
            return this;
        }

        InitArg setDynamicParameters(Map<String, String> dynamicParameters) {
            this.dynamicParameters = dynamicParameters;
            return this;
        }

        InitArg setDependencies(List<YTreeNode> dependencies) {
            this.dependencies = dependencies;
            return this;
        }

        InitArg setResourceClass(String resourceClass) {
            this.resourceClass = resourceClass;
            return this;
        }

        ByteString build() {
            YTreeBuilder specParameters = YTree.builder().beginMap()
                    .key("companion_resource_class").value(resourceClass);
            parameters.forEach((key, value) -> specParameters.key(key).value(value));

            YTreeBuilder dynamicSpecParameters = YTree.builder().beginMap();
            dynamicParameters.forEach((key, value) -> dynamicSpecParameters.key(key).value(value));

            YTreeBuilder dependenciesList = YTree.builder().beginList();
            dependencies.forEach(dependenciesList::value);

            return YsonUtils.protoFromYTree(YTree.builder().beginMap()
                    .key("spec").beginMap()
                    .key("resource_class_name").value("WorkerProxy")
                    .key("parameters").value(specParameters.endMap().build())
                    .endMap()
                    .key("dynamic_spec").beginMap()
                    .key("parameters").value(dynamicSpecParameters.endMap().build())
                    .endMap()
                    .key("incarnation_id").value(incarnationId.toString())
                    .key("incarnation_generation").value(incarnationGeneration)
                    .key("configuration_generation").value(configurationGeneration)
                    .key("dependencies").value(dependenciesList.endList().build())
                    .endMap().build());
        }
    }
}

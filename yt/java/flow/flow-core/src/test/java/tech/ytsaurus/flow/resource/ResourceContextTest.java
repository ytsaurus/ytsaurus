package tech.ytsaurus.flow.resource;

import java.util.Map;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.ysontree.YTree;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ResourceContextTest {

    private static class PoolResource implements FlowResource {
        @Override
        public void load(ResourceContext context) {
        }

        @Override
        public void unload() {
            // No external resources to release.
        }
    }

    private static class OtherResource implements FlowResource {
        @Override
        public void load(ResourceContext context) {
        }

        @Override
        public void unload() {
            // No external resources to release.
        }
    }

    private static ResourceContext contextWith(Map<String, FlowResource> dependencies) {
        return new ResourceContext("r", YTree.mapBuilder().buildMap(), YTree.mapBuilder().buildMap(), dependencies);
    }

    @Test
    public void dependencyReturnsTheTypedInstance() {
        var pool = new PoolResource();
        var context = contextWith(Map.of("pool", pool));

        assertSame(pool, context.dependency("pool", PoolResource.class));
    }

    @Test
    public void dependencyRejectsAMissingAlias() {
        var context = contextWith(Map.of());

        var error = assertThrows(IllegalStateException.class,
                () -> context.dependency("pool", PoolResource.class));
        assertTrue(error.getMessage().contains("pool"));
    }

    @Test
    public void dependencyRejectsAWrongType() {
        var context = contextWith(Map.of("pool", new OtherResource()));

        var error = assertThrows(IllegalStateException.class,
                () -> context.dependency("pool", PoolResource.class));
        assertTrue(error.getMessage().contains("pool"));
        assertTrue(error.getMessage().contains(PoolResource.class.getName()));
        assertTrue(error.getMessage().contains(OtherResource.class.getName()));
    }
}

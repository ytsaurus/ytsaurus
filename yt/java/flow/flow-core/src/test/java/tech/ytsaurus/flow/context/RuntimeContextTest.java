package tech.ytsaurus.flow.context;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.flow.resource.FlowResource;
import tech.ytsaurus.flow.resource.ResourceContext;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class RuntimeContextTest {

    private static class ViewResource implements FlowResource {
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

    @Test
    public void getResourceDefaultsToUnsupported() {
        // Runtime contexts that do not host resources — including user test
        // doubles written against the interface — must not be forced to
        // implement the accessor.
        RuntimeContext context = mock(RuntimeContext.class, CALLS_REAL_METHODS);

        assertThrows(UnsupportedOperationException.class, () -> context.getResource("view"));
    }

    @Test
    public void typedGetResourceReturnsTheInstance() {
        var view = new ViewResource();
        RuntimeContext context = mock(RuntimeContext.class, CALLS_REAL_METHODS);
        doReturn(view).when(context).getResource("view");

        assertSame(view, context.getResource("view", ViewResource.class));
    }

    @Test
    public void typedGetResourceRejectsAWrongType() {
        RuntimeContext context = mock(RuntimeContext.class, CALLS_REAL_METHODS);
        doReturn(new OtherResource()).when(context).getResource("view");

        var error = assertThrows(IllegalStateException.class,
                () -> context.getResource("view", ViewResource.class));
        assertTrue(error.getMessage().contains("view"));
        assertTrue(error.getMessage().contains(ViewResource.class.getName()));
        assertTrue(error.getMessage().contains(OtherResource.class.getName()));
    }
}

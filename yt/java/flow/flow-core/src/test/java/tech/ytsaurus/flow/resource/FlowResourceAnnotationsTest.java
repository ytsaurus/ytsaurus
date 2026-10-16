package tech.ytsaurus.flow.resource;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FlowResourceAnnotationsTest {

    static class PlainResource implements FlowResource {
        @Override
        public void load(ResourceContext context) {
        }

        @Override
        public void unload() {
            // No external resources to release.
        }
    }

    @FlowResourceClass("named_resource")
    static class NamedResource implements FlowResource {
        @Override
        public void load(ResourceContext context) {
        }

        @Override
        public void unload() {
            // No external resources to release.
        }
    }

    @FlowResourceClass
    static class BlankNamedResource implements FlowResource {
        @Override
        public void load(ResourceContext context) {
        }

        @Override
        public void unload() {
            // No external resources to release.
        }
    }

    @Test
    public void plainClassIsNamedByItsFullyQualifiedName() {
        assertEquals(PlainResource.class.getName(),
                FlowResourceAnnotations.resourceClassName(PlainResource.class));
    }

    @Test
    public void annotationValueOverridesTheName() {
        assertEquals("named_resource",
                FlowResourceAnnotations.resourceClassName(NamedResource.class));
    }

    @Test
    public void blankAnnotationValueFallsBackToTheFullyQualifiedName() {
        assertEquals(BlankNamedResource.class.getName(),
                FlowResourceAnnotations.resourceClassName(BlankNamedResource.class));
    }
}

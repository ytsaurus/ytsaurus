package tech.ytsaurus.flow.stream;

import java.util.List;

import javax.persistence.Entity;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.flow.row.FlowMessage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FlowStreamAnnotationsTest {

    @Entity
    @FlowMessage(streamIds = {"a", "b"})
    private static class MultiStreamMessage {
        private String word;
        private long count;
    }

    @Entity
    @FlowMessage(streamIds = {})
    private static class NoIdMessage {
        private String word;
    }

    @Entity
    private static class NotAnnotatedMessage {
        private String word;
    }

    @FlowMessage(streamIds = {"c"})
    private static class NotEntityMessage {
        private String word;
    }

    @Test
    public void fromAnnotatedClassBuildsOneStreamPerId() {
        List<FlowStream<?>> streams = FlowStreamAnnotations.fromAnnotatedClass(MultiStreamMessage.class);

        assertEquals(2, streams.size());
        assertEquals("a", streams.get(0).getStreamId());
        assertEquals("b", streams.get(1).getStreamId());
        for (FlowStream<?> stream : streams) {
            assertEquals(MultiStreamMessage.class, stream.getMessageClass());
            assertNotNull(stream.getSchema());
        }
    }

    @Test
    public void fromAnnotatedClassRejectsEmptyStreamIds() {
        assertThrows(IllegalArgumentException.class,
                () -> FlowStreamAnnotations.fromAnnotatedClass(NoIdMessage.class));
    }

    @Test
    public void fromAnnotatedClassRejectsMissingAnnotation() {
        assertThrows(IllegalArgumentException.class,
                () -> FlowStreamAnnotations.fromAnnotatedClass(NotAnnotatedMessage.class));
    }

    @Test
    public void fromAnnotatedClassPropagatesMissingEntity() {
        assertThrows(RuntimeException.class,
                () -> FlowStreamAnnotations.fromAnnotatedClass(NotEntityMessage.class));
    }

    @Test
    public void fromAnnotatedClassesFlatMapsAllClasses() {
        List<FlowStream<?>> streams = FlowStreamAnnotations.fromAnnotatedClasses(List.of(MultiStreamMessage.class));

        assertEquals(2, streams.size());
    }
}

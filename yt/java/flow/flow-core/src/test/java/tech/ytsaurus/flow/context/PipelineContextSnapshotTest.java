package tech.ytsaurus.flow.context;

import java.util.List;
import java.util.Set;

import javax.persistence.Entity;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.flow.row.FlowMessage;
import tech.ytsaurus.flow.stream.FlowStreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PipelineContextSnapshotTest {

    @Test
    public void testGetStreams() {
        var context = new PipelineContext();
        context.registerStream(FlowStreams.typed("words", Word.class));
        context.registerStream(FlowStreams.typed("unique-words", Word.class));

        var snapshot = new PipelineContextSnapshot(context);

        assertEquals(Set.of("words", "unique-words"), snapshot.getStreams().keySet());
        assertEquals(
                context.getStreams().get("words").getSchema(),
                snapshot.getStreams().get("words").getSchema());
    }

    @Test
    public void testSnapshotIsFrozenAtCreation() {
        var context = new PipelineContext();
        context.registerStream(FlowStreams.typed("words", Word.class));

        var snapshot = new PipelineContextSnapshot(context);
        context.registerStream(FlowStreams.typed("late-words", Word.class));

        // The snapshot is a point-in-time copy.
        assertEquals(Set.of("words"), snapshot.getStreams().keySet());
    }

    @Test
    public void testStreamsKeepRegistrationOrder() {
        var context = new PipelineContext();
        for (String id : new String[]{"zeta", "alpha", "mid", "beta"}) {
            context.registerStream(FlowStreams.typed(id, Word.class));
        }

        var snapshot = new PipelineContextSnapshot(context);

        // Registration order reaches the spec, so it must be deterministic.
        assertEquals(
                List.of("zeta", "alpha", "mid", "beta"),
                List.copyOf(snapshot.getStreams().keySet()));
    }

    @Test
    public void testGetStreamsIsUnmodifiable() {
        var snapshot = new PipelineContextSnapshot(new PipelineContext());

        assertThrows(
                UnsupportedOperationException.class,
                () -> snapshot.getStreams().put("words", FlowStreams.typed("words", Word.class)));
    }

    @Entity
    @FlowMessage(streamIds = {"words"})
    private static class Word {
        private String word;
    }
}

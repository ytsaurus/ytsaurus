package tech.ytsaurus.flow.job;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.flow.stream.StreamIdsMapping;
import tech.ytsaurus.flow.stream.StreamSpecs;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobTest {
    // The C++ runtime returns 2^64 - 1 for a computation without input streams.
    private static final long CPP_INFINITY = 0xffffffffffffffffL;

    private static Job job(YTreeNode staticSpec) {
        return new Job(
                GUID.create(),
                "computation",
                new StreamSpecs(StreamIdsMapping.builder().build(), List.of()),
                staticSpec,
                YTree.mapBuilder().buildMap(),
                null);
    }

    private static Job jobWithInputStreams(String... streamIds) {
        YTreeBuilder builder = YTree.mapBuilder().key("input_stream_ids").beginList();
        for (var streamId : streamIds) {
            builder.value(streamId);
        }
        return job(builder.endList().buildMap());
    }

    @Test
    void testMinWatermarkCoversOnlyInputStreams() {
        var job = jobWithInputStreams("clicks", "shows");

        assertEquals(200L, job.getMinWatermark(Map.of("clicks", 300L, "shows", 200L, "timers", 100L)));
    }

    @Test
    void testMinWatermarkCountsMissingInputStreamAsZero() {
        assertEquals(0L, jobWithInputStreams("clicks", "shows").getMinWatermark(Map.of("clicks", 300L)));
    }

    @Test
    void testMinWatermarkIsLongMaxValueWithoutInputStreams() {
        long minWatermark = jobWithInputStreams().getMinWatermark(Map.of("source", 300L));

        assertEquals(Long.MAX_VALUE, minWatermark);
        // Every event is late, as against the C++ value under an unsigned comparison.
        for (long eventTimestamp : new long[]{0L, 1_795_372_633_000_000L, Long.MAX_VALUE - 1}) {
            assertTrue(eventTimestamp < minWatermark);
            assertTrue(Long.compareUnsigned(eventTimestamp, CPP_INFINITY) < 0);
        }
    }

    @Test
    void testMinWatermarkSaturatesWatermarksAboveLongMaxValue() {
        var job = jobWithInputStreams("clicks", "shows");

        // A wire uint64 of 2^64 - 1 arrives as -1 and must not become the minimum.
        assertEquals(200L, job.getMinWatermark(Map.of("clicks", CPP_INFINITY, "shows", 200L)));
        assertEquals(Long.MAX_VALUE, job.getMinWatermark(Map.of("clicks", CPP_INFINITY, "shows", Long.MIN_VALUE)));
    }

    @Test
    void testMinWatermarkTakesEveryWatermarkWithoutInputStreamIds() {
        var job = job(YTree.mapBuilder().buildMap());

        assertEquals(100L, job.getMinWatermark(Map.of("clicks", 300L, "timers", 100L)));
        assertEquals(0L, job.getMinWatermark(Map.of()));
    }
}

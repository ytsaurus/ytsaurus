package tech.ytsaurus.flow.pipeline;

import java.util.Map;

import javax.persistence.Entity;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.FlowMessage;
import tech.ytsaurus.flow.stream.FlowStream;
import tech.ytsaurus.flow.stream.FlowStreams;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineSpecEnricherTest {

    private static final FlowStream<?> WORDS = FlowStreams.typed("words", Word.class);

    @Test
    void testAddsMissingStreamsBlock() {
        YTreeMapNode spec = YTree.mapBuilder().buildMap();

        PipelineSpecEnricher.patchStreamSchemas(spec, Map.of(WORDS.getStreamId(), WORDS));

        assertEquals(WORDS.getSchema(), schemaOf(spec, "words"));
    }

    @Test
    void testFillsSchemaOfDeclaredStream() {
        YTreeMapNode spec = parse("""
                {
                    "streams" = {
                        "words" = {
                            "migration_function" = "identity";
                        };
                    };
                }
                """);

        PipelineSpecEnricher.patchStreamSchemas(spec, Map.of(WORDS.getStreamId(), WORDS));

        assertEquals(WORDS.getSchema(), schemaOf(spec, "words"));
        // The rest of the stream spec survives.
        assertEquals(
                "identity",
                spec.getOrThrow("streams").mapNode()
                        .getOrThrow("words").mapNode()
                        .getOrThrow("migration_function").stringValue());
    }

    @Test
    void testKeepsMatchingSchema() {
        YTreeMapNode spec = YTree.mapBuilder().buildMap();
        YTreeMapNode streams = YTree.mapBuilder().buildMap();
        YTreeMapNode words = YTree.mapBuilder().buildMap();
        words.put("schema", WORDS.getSchema().toYTree());
        streams.put("words", words);
        spec.put("streams", streams);

        PipelineSpecEnricher.patchStreamSchemas(spec, Map.of(WORDS.getStreamId(), WORDS));

        assertEquals(WORDS.getSchema(), schemaOf(spec, "words"));
    }

    @Test
    void testUnparsableSchemaIsKept() {
        YTreeMapNode spec = parse("""
                {
                    "streams" = {
                        "words" = {
                            "schema" = [{name = "word"; type = "not_a_type";}];
                        };
                    };
                }
                """);

        PipelineSpecEnricher.patchStreamSchemas(spec, Map.of(WORDS.getStreamId(), WORDS));

        assertEquals(
                "not_a_type",
                spec.getOrThrow("streams").mapNode()
                        .getOrThrow("words").mapNode()
                        .getOrThrow("schema").listNode().get(0).mapNode()
                        .getOrThrow("type").stringValue());
    }

    @Test
    void testKeepsDivergingSchemaFromConfig() {
        // The config is authoritative at runtime, so a declared type that differs from the derived
        // one is kept: a Java String field derives utf8, while specs commonly declare string.
        YTreeMapNode spec = parse("""
                {
                    "streams" = {
                        "words" = {
                            "schema" = [{name = "word"; type = "string";}];
                        };
                    };
                }
                """);

        PipelineSpecEnricher.patchStreamSchemas(spec, Map.of(WORDS.getStreamId(), WORDS));

        assertEquals(
                TableSchema.fromYTree(parse("""
                        {"schema" = [{name = "word"; type = "string";}];}
                        """).getOrThrow("schema")),
                schemaOf(spec, "words"));
    }

    @Test
    void testNoStreamsLeavesSpecUntouched() {
        YTreeMapNode spec = YTree.mapBuilder().buildMap();

        PipelineSpecEnricher.patchStreamSchemas(spec, Map.of());

        assertFalse(spec.containsKey("streams"));
    }

    @Test
    void testValidateRejectsCompanionWithoutMainClass() {
        // The worker spawns the companion whenever the resource is required on it, so every
        // companion resource needs a class to start.
        YTreeMapNode spec = parse("""
                {
                    "resources" = {
                        "CompanionManager" = {
                            "resource_class_name" = "NYT::NFlow::NCompanion::TJavaCompanionManager";
                            "parameters" = {};
                        };
                    };
                }
                """);

        var error = assertThrows(
                IllegalStateException.class,
                () -> PipelineSpecEnricher.validateCompanionMainClass(spec));
        assertTrue(error.getMessage().contains("CompanionManager"));
        assertTrue(error.getMessage().contains("main_class"));
    }

    @Test
    void testValidateRejectsBlankMainClass() {
        // main_class defaults to an empty string on the C++ side, so a blank one is as good as none.
        YTreeMapNode spec = parse("""
                {
                    "resources" = {
                        "CompanionManager" = {
                            "resource_class_name" = "NYT::NFlow::NCompanion::TJavaCompanionManager";
                            "parameters" = {"main_class" = "  ";};
                        };
                    };
                }
                """);

        assertThrows(
                IllegalStateException.class,
                () -> PipelineSpecEnricher.validateCompanionMainClass(spec));
    }

    @Test
    void testValidateAcceptsDeclaredMainClass() {
        YTreeMapNode spec = parse("""
                {
                    "resources" = {
                        "CompanionManager" = {
                            "resource_class_name" = "NYT::NFlow::NCompanion::TJavaCompanionManager";
                            "parameters" = {"main_class" = "tech.ytsaurus.flow.tests.PipelineMain";};
                        };
                    };
                }
                """);

        PipelineSpecEnricher.validateCompanionMainClass(spec);
    }

    @Test
    void testRejectsNonMapStreamsNode() {
        // Repairing a malformed node would submit a different spec than the one written.
        YTreeMapNode spec = parse("""
                {
                    "streams" = "not a map";
                }
                """);

        var error = assertThrows(
                IllegalArgumentException.class,
                () -> PipelineSpecEnricher.patchStreamSchemas(spec, Map.of(WORDS.getStreamId(), WORDS)));
        assertTrue(error.getMessage().contains("streams"));
    }

    @Test
    void testValidateSkipsNonJavaCompanionResources() {
        // Only the Java companion manager needs a class to start.
        YTreeMapNode spec = parse("""
                {
                    "resources" = {
                        "CompanionManager" = {
                            "resource_class_name" = "NYT::NFlow::NCompanion::TCompanionManager";
                            "parameters" = {};
                        };
                    };
                }
                """);

        PipelineSpecEnricher.validateCompanionMainClass(spec);
    }

    private static YTreeMapNode parse(String yson) {
        return YTreeTextSerializer.deserialize(yson).mapNode();
    }

    private static TableSchema schemaOf(YTreeMapNode spec, String streamId) {
        return TableSchema.fromYTree(spec
                .getOrThrow("streams").mapNode()
                .getOrThrow(streamId).mapNode()
                .getOrThrow("schema"));
    }

    @Entity
    @FlowMessage(streamIds = {"words"})
    private static class Word {
        private String word;
    }
}

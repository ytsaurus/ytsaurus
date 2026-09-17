package tech.ytsaurus.flow.context;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import javax.persistence.Entity;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.computation.SourceComputation;
import tech.ytsaurus.flow.function.BatchFunction;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.resource.FlowResource;
import tech.ytsaurus.flow.resource.FlowResourceClass;
import tech.ytsaurus.flow.resource.ResourceContext;
import tech.ytsaurus.flow.row.FlowMessage;
import tech.ytsaurus.flow.state.StateDescriptor;
import tech.ytsaurus.flow.state.StateDescriptors;
import tech.ytsaurus.flow.stream.FlowStreams;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PipelineContextTest {

    @Test
    public void testYTree() {
        var expected = YTreeTextSerializer.deserialize("""
                {
                    computations={
                        "computation_id_1"={
                            computation_id="computation_id_1";
                            computation_type="Source";
                            supported_state_formats=["simple_row";"proto";];
                        };
                        "computation_id_2"={
                            computation_id="computation_id_2";
                            computation_type="Source";
                            supported_state_formats=["simple_row";"proto";];
                        };
                        "computation_id_3"={
                            computation_id="computation_id_3";
                            computation_type="Transform";
                            supported_state_formats=["simple_row";"proto";];
                        };
                    };
                    resource_classes=[];
                }""".stripIndent());
        var context = new PipelineContext();
        var sourceComputation = SourceComputation.builder()
                .setComputationId("computation_id_1")
                .setProcessFunction((BatchFunction) (messages, output, ctx) -> {
                    // Batch passthrough.
                    messages.forEach(output::addMessage);
                })
                .build();
        context.registerComputation(sourceComputation);
        var rowSourceComputation = SourceComputation.builder()
                .setComputationId("computation_id_2")
                .setProcessFunction((RowFunction) (message, output, ctx) -> {
                    // Passthrough.
                    output.addMessage(message);
                })
                .build();
        context.registerComputation(rowSourceComputation);
        var transformComputation = Computation.builder()
                .setComputationId("computation_id_3")
                .setProcessFunction((RowFunction) (message, output, ctx) -> {
                    // Passthrough.
                    output.addMessage(message);
                })
                .build();
        context.registerComputation(transformComputation);
        var expectedComputationsMap = YTreeTextSerializer.stableSerialize(expected);
        var actualComputationsMap = YTreeTextSerializer.stableSerialize(new PipelineContextSnapshot(context).toYTree());
        assertEquals(expectedComputationsMap, actualComputationsMap);
    }

    @Test
    public void nullProcessFunctionIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> SourceComputation.builder()
                .setComputationId("source")
                .build());
        assertThrows(IllegalArgumentException.class, () -> Computation.builder()
                .setComputationId("transform")
                .build());
    }

    @Test
    public void registerStreamsRegistersEveryStream() {
        var context = new PipelineContext();
        var schema = TableSchema.builder()
                .addValue("value", TiType.string())
                .build();

        context.registerStreams(List.of(
                FlowStreams.raw("stream_id_1", schema),
                FlowStreams.raw("stream_id_2", schema)
        ));

        assertEquals(2, context.getStreams().size());
        assertEquals("stream_id_1", context.getStreams().get("stream_id_1").getStreamId());
        assertEquals("stream_id_2", context.getStreams().get("stream_id_2").getStreamId());
    }

    @Test
    public void registerStreamsRejectsDuplicateId() {
        var context = new PipelineContext();
        var schema = TableSchema.builder()
                .addValue("value", TiType.string())
                .build();
        context.registerStream(FlowStreams.raw("stream_id_1", schema));

        assertThrows(IllegalArgumentException.class, () -> context.registerStreams(List.of(
                FlowStreams.raw("stream_id_1", schema)
        )));
    }

    @Entity
    @FlowMessage(streamIds = {"typed_a", "typed_b"})
    private static class MultiStreamMessage {
        private String word;
        private long count;
    }

    @Entity
    @FlowMessage(streamIds = {"typed_a"})
    private static class DuplicateIdMessage {
        private String word;
    }

    @Test
    public void registerTypedStreamsRegistersEveryDeclaredId() {
        var context = new PipelineContext();

        context.registerTypedStreams(MultiStreamMessage.class);

        assertEquals(2, context.getStreams().size());
        assertEquals(MultiStreamMessage.class, context.getStreams().get("typed_a").getMessageClass());
        assertEquals(MultiStreamMessage.class, context.getStreams().get("typed_b").getMessageClass());
    }

    @Test
    public void registerTypedStreamsRejectsDuplicateIdAcrossClasses() {
        var context = new PipelineContext();
        context.registerTypedStreams(MultiStreamMessage.class);

        assertThrows(IllegalArgumentException.class,
                () -> context.registerTypedStreams(DuplicateIdMessage.class));
    }

    @Test
    public void snapshotDoesNotObserveLaterContextChanges() {
        var context = new PipelineContext();
        var schema = TableSchema.builder()
                .addValue("value", TiType.string())
                .build();
        context.registerComputation(Computation.builder()
                .setComputationId("computation_id_1")
                .setProcessFunction((RowFunction) (message, output, ctx) -> output.addMessage(message))
                .build());
        context.registerStream(FlowStreams.raw("stream_id_1", schema));

        var snapshot = new PipelineContextSnapshot(context);

        context.registerComputation(Computation.builder()
                .setComputationId("computation_id_2")
                .setProcessFunction((RowFunction) (message, output, ctx) -> output.addMessage(message))
                .build());
        context.registerStream(FlowStreams.raw("stream_id_2", schema));

        assertNull(snapshot.getComputation("computation_id_2"));
        assertNull(snapshot.getStreamContext().getStream("stream_id_2"));
    }

    @Test
    public void registerStatesRegistersEveryStateInOrder() {
        var context = new PipelineContext();
        var state = StateDescriptors.external("/state");

        context.registerState(state);
        context.registerStates(List.of(
                StateDescriptors.externalReadOnly("/joined-state"),
                StateDescriptors.raw("counter")
        ));

        assertEquals(
                List.of("/state", "/joined-state", "counter"),
                context.getStates().stream().map(StateDescriptor::getName).toList());
        assertSame(state, context.getStates().get(0));
    }

    @Test
    public void registerStateIgnoresTheSameDescriptorTwice() {
        var context = new PipelineContext();
        var state = StateDescriptors.external("/state");

        context.registerState(state);
        context.registerState(state);

        assertEquals(List.of(state), context.getStates());
    }

    @Test
    public void registerStateKeepsDescriptorsOfOneNameAndType() {
        // The owner of an external state and the computation joining it declare it under one name.
        var context = new PipelineContext();
        var owned = StateDescriptors.external("/state");
        var joined = StateDescriptors.externalReadOnly("/state");

        context.registerState(owned);
        context.registerStates(List.of(joined, StateDescriptors.external("/state")));

        assertEquals(3, context.getStates().size());
        assertSame(owned, context.getStates().get(0));
        assertSame(joined, context.getStates().get(1));
    }

    @Test
    public void registerStateRejectsAnotherTypeUnderOneName() {
        var context = new PipelineContext();
        context.registerState(StateDescriptors.external("/state"));

        var error = assertThrows(
                IllegalArgumentException.class, () -> context.registerState(StateDescriptors.raw("/state")));
        assertTrue(error.getMessage().contains("/state"));
        assertThrows(NullPointerException.class, () -> context.registerState((StateDescriptor<?>) null));
    }

    @Test
    public void registerResourceClassRejectsDuplicateName() {
        var context = new PipelineContext();
        context.registerResourceClass("resource_class_1", TestResource::new);

        assertThrows(IllegalArgumentException.class,
                () -> context.registerResourceClass("resource_class_1", TestResource::new));
    }

    @Test
    public void registerResourceClassDerivesTheResourceClassName() {
        var context = new PipelineContext();
        context.registerResourceClass(TestResource.class, TestResource::new);
        context.registerResourceClass(AnnotatedResource.class, AnnotatedResource::new);

        assertTrue(context.getResourceFactories().containsKey(TestResource.class.getName()));
        assertTrue(context.getResourceFactories().containsKey("annotated_resource"));
    }

    @Test
    public void registerResourceClassRejectsDuplicateDerivedName() {
        var context = new PipelineContext();
        context.registerResourceClass(TestResource.class, TestResource::new);

        assertThrows(IllegalArgumentException.class,
                () -> context.registerResourceClass(TestResource.class, TestResource::new));
    }

    @Test
    public void snapshotDoesNotObserveLaterResourceClassRegistrations() {
        var context = new PipelineContext();
        context.registerResourceClass("resource_class_1", TestResource::new);

        var snapshot = new PipelineContextSnapshot(context);

        context.registerResourceClass("resource_class_2", TestResource::new);

        assertTrue(snapshot.getResourceFactories().containsKey("resource_class_1"));
        assertFalse(snapshot.getResourceFactories().containsKey("resource_class_2"));
    }

    @Test
    public void resourceClassesKeepRegistrationOrder() {
        var context = new PipelineContext();
        for (String name : new String[]{"zeta", "alpha", "mid", "beta"}) {
            context.registerResourceClass(name, TestResource::new);
        }

        var snapshot = new PipelineContextSnapshot(context);

        // Registration order reaches the spec, so it must be deterministic.
        assertEquals(
                List.of("zeta", "alpha", "mid", "beta"),
                List.copyOf(snapshot.getResourceFactories().keySet()));
    }

    @Test
    public void explicitResourceClassNameDoesNotReadTheAnnotation() {
        var context = new PipelineContext();
        Supplier<AnnotatedResource> factory = AnnotatedResource::new;
        context.registerResourceClass("explicit_name", factory);

        assertEquals(List.of("explicit_name"), List.copyOf(context.getResourceFactories().keySet()));
        assertSame(factory, context.getResourceFactories().get("explicit_name"));
    }

    @Test
    public void emptyAnnotationUsesTheFullyQualifiedClassName() {
        var context = new PipelineContext();
        context.registerResourceClass(DefaultNamedResource.class, DefaultNamedResource::new);

        assertTrue(context.getResourceFactories().containsKey(DefaultNamedResource.class.getName()));
    }

    @Test
    public void registrationAndSnapshotDoNotInvokeResourceFactories() {
        var context = new PipelineContext();
        var creations = new AtomicInteger();
        Supplier<TestResource> plainFactory = () -> {
            creations.incrementAndGet();
            return new TestResource();
        };
        Supplier<AnnotatedResource> annotatedFactory = () -> {
            creations.incrementAndGet();
            return new AnnotatedResource();
        };
        context.registerResourceClass("explicit", plainFactory);
        context.registerResourceClass(TestResource.class, plainFactory);
        context.registerResourceClass(AnnotatedResource.class, annotatedFactory);
        var snapshot = new PipelineContextSnapshot(context);

        assertEquals(3, snapshot.getResourceFactories().size());
        assertEquals(0, creations.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void duplicateNamesAreRejectedAcrossOverloadsWithoutReplacingTheFactory(boolean classFirst) {
        var context = new PipelineContext();
        Supplier<AnnotatedResource> factory = AnnotatedResource::new;
        Supplier<AnnotatedResource> duplicate = () -> {
            throw new AssertionError("Registration must not invoke a factory");
        };
        if (classFirst) {
            context.registerResourceClass(AnnotatedResource.class, factory);
            assertThrows(IllegalArgumentException.class,
                    () -> context.registerResourceClass("annotated_resource", duplicate));
        } else {
            context.registerResourceClass("annotated_resource", factory);
            assertThrows(IllegalArgumentException.class,
                    () -> context.registerResourceClass(AnnotatedResource.class, duplicate));
        }
        assertEquals(1, context.getResourceFactories().size());
        assertSame(factory, context.getResourceFactories().get("annotated_resource"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "\t\r\n", "\u2003"})
    public void blankResourceClassNamesAreRejectedBeforeRegistration(String name) {
        var context = new PipelineContext();
        var error = assertThrows(IllegalArgumentException.class,
                () -> context.registerResourceClass(name, TestResource::new));

        assertTrue(error.getMessage().contains("must not be blank"));
        assertTrue(new PipelineContextSnapshot(context).getResourceFactories().isEmpty());
    }

    @Test
    public void whitespaceAnnotationIsRejectedByTheSameValidation() {
        var context = new PipelineContext();
        assertThrows(IllegalArgumentException.class,
                () -> context.registerResourceClass(WhitespaceNamedResource.class, WhitespaceNamedResource::new));
        assertTrue(context.getResourceFactories().isEmpty());
    }

    @Test
    public void nullResourceRegistrationArgumentsDoNotMutateTheRegistry() {
        var context = new PipelineContext();
        assertThrows(NullPointerException.class,
                () -> context.registerResourceClass((String) null, TestResource::new));
        assertThrows(NullPointerException.class,
                () -> context.registerResourceClass((Class<TestResource>) null, TestResource::new));
        assertThrows(NullPointerException.class, () -> context.registerResourceClass("valid", null));
        assertThrows(NullPointerException.class, () -> context.registerResourceClass(TestResource.class, null));
        assertTrue(context.getResourceFactories().isEmpty());

        Supplier<TestResource> factory = TestResource::new;
        context.registerResourceClass("valid", factory);
        assertThrows(NullPointerException.class, () -> context.registerResourceClass("valid", null));
        assertSame(factory, context.getResourceFactories().get("valid"));
    }

    @FlowResourceClass
    static class DefaultNamedResource extends TestResource {
    }

    @FlowResourceClass(" \t")
    static class WhitespaceNamedResource extends TestResource {
    }

    static class TestResource implements FlowResource {
        @Override
        public void load(ResourceContext context) {
        }

        @Override
        public void unload() {
            // No external resources to release.
        }
    }

    @FlowResourceClass("annotated_resource")
    static class AnnotatedResource implements FlowResource {
        @Override
        public void load(ResourceContext context) {
        }

        @Override
        public void unload() {
            // No external resources to release.
        }
    }
}

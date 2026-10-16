package tech.ytsaurus.flow.pipeline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.state.ProtoStateDescriptor;
import tech.ytsaurus.flow.state.StateDescriptor;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Describes the profile states of the pipeline spec from the states the pipeline declared: a
 * {@code descriptor_layout} without {@code descriptor_set} receives the self-contained descriptor
 * set of the declared message and its name. The worker is the stock {@code flow_server}, which
 * never compiles the user's proto, so the descriptors travel in the spec.
 *
 * <p>A spec entry is described from the declared state of its name; the owner of a state and the
 * computations joining it declare one message under one name. A state that already carries a
 * {@code layout} or a {@code descriptor_set} is left alone, and so is one whose
 * {@code message_name} differs from the declared message or is declared nowhere: the worker then
 * resolves the message from its own generated descriptor pool, the way an in-process C++ pipeline
 * does. Other manager classes are not touched.
 */
final class ProfileStateSpecEnricher {

    private static final Logger log = LoggerFactory.getLogger(ProfileStateSpecEnricher.class);

    /** The C++ profile state manager and joiner the descriptor source is filled for. */
    static final String PROFILE_STATE_MANAGER_CLASS = "NYT::NFlow::NProfileState::TProfileStateManager";
    static final String PROFILE_STATE_JOINER_CLASS = "NYT::NFlow::NProfileState::TProfileStateJoiner";

    /** The cap the C++ spec parser puts on a serialized descriptor set. */
    static final int MAX_DESCRIPTOR_SET_SIZE = 8 << 20;

    private static final String KEY_COMPUTATIONS = "computations";
    /** The alias the C++ spec parser accepts for either class key. */
    private static final String KEY_CLASS_NAME_ALIAS = "class_name";
    private static final String KEY_PARAMETERS = "parameters";
    private static final String KEY_LAYOUT = "layout";
    private static final String KEY_DESCRIPTOR_LAYOUT = "descriptor_layout";
    private static final String KEY_DESCRIPTOR_SET = "descriptor_set";
    private static final String KEY_MESSAGE_NAME = "message_name";

    private static final List<ProfileStateKind> PROFILE_STATE_KINDS = List.of(
            new ProfileStateKind(
                    "external_state_managers", "external_state_manager_class_name", PROFILE_STATE_MANAGER_CLASS,
                    "StateDescriptors.externalProto(...)"),
            new ProfileStateKind(
                    "external_state_joiners", "external_state_joiner_class_name", PROFILE_STATE_JOINER_CLASS,
                    "StateDescriptors.externalProtoReadOnly(...)"));

    private ProfileStateSpecEnricher() {
    }

    /**
     * Describes every profile state manager and joiner of the spec from the declared states.
     *
     * @param spec   the {@code spec} map of the pipeline config; patched in place.
     * @param states the states declared by the pipeline.
     * @throws IllegalArgumentException if a profile state has no descriptor source and no declared
     *                                  proto state of its name, if several messages are declared
     *                                  under its name, if its descriptor set is over the limit of
     *                                  {@code flow_server}, or if a node of the entry has the wrong
     *                                  type.
     */
    static void patch(YTreeMapNode spec, Collection<StateDescriptor<?>> states) {
        DeclaredMessages declared = new DeclaredMessages(states);
        for (ProfileStateEntry entry : profileStates(spec)) {
            describe(entry, declared);
        }
    }

    /** A spec section holding profile states, with the class key (and its alias) selecting them. */
    private record ProfileStateKind(String sectionKey, String classKey, String profileClass, String factoryHint) {
        /** Adds the profile states of the section of the computation to {@code into}. */
        void collect(String computationId, YTreeMapNode computation, List<ProfileStateEntry> into) {
            PipelineSpecEnricher.mapNode(computation, sectionKey).ifPresent(section ->
                    section.asMap().forEach((stateName, stateSpec) -> {
                        if (stateSpec.isMapNode() && profileClass.equals(className(stateSpec.mapNode()))) {
                            into.add(new ProfileStateEntry(computationId, stateName, this, stateSpec.mapNode()));
                        }
                    }));
        }

        private String className(YTreeMapNode stateSpec) {
            return stringValue(stateSpec, classKey)
                    .or(() -> stringValue(stateSpec, KEY_CLASS_NAME_ALIAS))
                    .orElse("");
        }
    }

    /** One profile state of the spec. */
    private record ProfileStateEntry(
            String computationId, String stateName, ProfileStateKind kind, YTreeMapNode spec
    ) {
        @Override
        public String toString() {
            return "Profile state %s of computation %s".formatted(stateName, computationId);
        }
    }

    /** The profile states of every computation of the spec. */
    private static List<ProfileStateEntry> profileStates(YTreeMapNode spec) {
        List<ProfileStateEntry> entries = new ArrayList<>();
        PipelineSpecEnricher.mapNode(spec, KEY_COMPUTATIONS).ifPresent(computations ->
                computations.asMap().forEach((computationId, computation) -> {
                    if (computation.isMapNode()) {
                        withContext("Computation " + computationId, () -> {
                            for (ProfileStateKind kind : PROFILE_STATE_KINDS) {
                                kind.collect(computationId, computation.mapNode(), entries);
                            }
                            return null;
                        });
                    }
                }));
        return entries;
    }

    /** What the spec says about the descriptor source of a profile state. */
    private record DescriptorSource(boolean explicit, @Nullable String messageName) {
        static DescriptorSource of(YTreeMapNode stateSpec) {
            Optional<YTreeMapNode> parameters = PipelineSpecEnricher.mapNode(stateSpec, KEY_PARAMETERS);
            Optional<YTreeMapNode> layout = parameters
                    .flatMap(map -> PipelineSpecEnricher.mapNode(map, KEY_DESCRIPTOR_LAYOUT));
            // An empty string is what the C++ parser takes for an absent layout or descriptor set.
            boolean explicit = parameters.flatMap(map -> nonEmptyString(map, KEY_LAYOUT)).isPresent()
                    || layout.flatMap(map -> nonEmptyString(map, KEY_DESCRIPTOR_SET)).isPresent();
            String messageName = layout.flatMap(map -> nonEmptyString(map, KEY_MESSAGE_NAME)).orElse(null);
            return new DescriptorSource(explicit, messageName);
        }
    }

    /**
     * Fills the descriptor source of one profile state from the message declared under its name.
     * Reads first, writes last: a state that is left alone or rejected keeps its spec as written.
     */
    private static void describe(ProfileStateEntry entry, DeclaredMessages declared) {
        DescriptorSource source = withContext(entry.toString(), () -> DescriptorSource.of(entry.spec()));
        if (source.explicit()) {
            return;
        }
        Map<String, Descriptors.Descriptor> messages = declared.messages(entry.stateName());
        if (messages.isEmpty()) {
            if (source.messageName() != null) {
                log.info(
                        "{}: leaving message {} to the generated descriptor pool of flow_server, as no state of"
                                + " that name is declared; a Java state is declared with {} and registered in the"
                                + " pipeline{}",
                        entry, source.messageName(), entry.kind().factoryHint(), declared.elsewhere(entry.stateName()));
                return;
            }
            throw new IllegalArgumentException(
                    ("%s has no descriptor source: declare it with %s and register it in the pipeline, or set"
                            + " descriptor_set in the pipeline spec%s")
                            .formatted(entry, entry.kind().factoryHint(), declared.elsewhere(entry.stateName())));
        }
        if (messages.size() > 1) {
            throw new IllegalArgumentException(
                    ("%s is declared with several messages (%s); states of one name share a message, so give"
                            + " them different names")
                            .formatted(entry, String.join(", ", messages.keySet())));
        }
        Descriptors.Descriptor descriptor = messages.values().iterator().next();
        if (source.messageName() != null && !source.messageName().equals(descriptor.getFullName())) {
            // Another computation may own a state of that name with a message compiled into flow_server.
            log.warn(
                    "{}: leaving message {} to the generated descriptor pool of flow_server, which the stock binary"
                            + " cannot resolve: the pipeline declares {} under that name. If this entry is the Java"
                            + " state, fix message_name",
                    entry, source.messageName(), descriptor.getFullName());
            return;
        }

        byte[] descriptorSet = withContext(entry.toString(), () -> serializeDescriptorSet(descriptor.getFile()));
        YTreeMapNode target = PipelineSpecEnricher.getOrCreateMap(
                PipelineSpecEnricher.getOrCreateMap(entry.spec(), KEY_PARAMETERS), KEY_DESCRIPTOR_LAYOUT);
        target.put(KEY_MESSAGE_NAME, YTree.stringNode(descriptor.getFullName()));
        target.put(KEY_DESCRIPTOR_SET, YTree.bytesNode(descriptorSet));
        log.info("{}: described from the declared message {}", entry, descriptor.getFullName());
    }

    /** The messages of the declared proto states, by state name. */
    private static final class DeclaredMessages {
        private final Map<String, Map<String, Descriptors.Descriptor>> messages = new LinkedHashMap<>();
        // The declared states that carry no message, by state name.
        private final Map<String, List<String>> withoutMessage = new LinkedHashMap<>();

        DeclaredMessages(Collection<StateDescriptor<?>> states) {
            for (StateDescriptor<?> state : states) {
                if (state instanceof ProtoStateDescriptor protoState) {
                    Descriptors.Descriptor descriptor = protoState.getMessageDescriptor();
                    messages.computeIfAbsent(state.getName(), name -> new LinkedHashMap<>())
                            .putIfAbsent(descriptor.getFullName(), descriptor);
                } else {
                    withoutMessage.computeIfAbsent(state.getName(), name -> new ArrayList<>())
                            .add(state.getClass().getSimpleName());
                }
            }
        }

        /** The messages declared under the state name, keyed by message name. */
        Map<String, Descriptors.Descriptor> messages(String stateName) {
            return messages.getOrDefault(stateName, Map.of());
        }

        /**
         * How the state is declared without a message, as a clause for a diagnostic; when it is
         * not declared at all, the names of the declared proto states instead, and empty when
         * there are none.
         */
        String elsewhere(String stateName) {
            List<String> notes = new ArrayList<>();
            for (String descriptorClass : withoutMessage.getOrDefault(stateName, List.of())) {
                notes.add("as " + descriptorClass + ", which has no message");
            }
            if (!notes.isEmpty()) {
                return "; the pipeline declares it " + String.join(", ", notes);
            }
            return messages.isEmpty() ? "" : "; the declared proto states are " + String.join(", ", messages.keySet());
        }
    }

    /**
     * Serializes a self-contained descriptor set: the file and every transitive import,
     * dependencies first. Fails ahead of {@code flow_server} when the set is over its limit.
     */
    static byte[] serializeDescriptorSet(Descriptors.FileDescriptor file) {
        DescriptorProtos.FileDescriptorSet.Builder set = DescriptorProtos.FileDescriptorSet.newBuilder();
        collectFiles(file, new LinkedHashSet<>(), set);
        byte[] bytes = set.build().toByteArray();
        if (bytes.length > MAX_DESCRIPTOR_SET_SIZE) {
            List<String> files = set.getFileList().stream().map(DescriptorProtos.FileDescriptorProto::getName).toList();
            throw new IllegalArgumentException(
                    "The descriptor set of %s is %d bytes, over the %d-byte limit of flow_server; it holds %s"
                            .formatted(file.getName(), bytes.length, MAX_DESCRIPTOR_SET_SIZE, files));
        }
        return bytes;
    }

    private static void collectFiles(
            Descriptors.FileDescriptor file,
            Set<String> seen,
            DescriptorProtos.FileDescriptorSet.Builder set
    ) {
        if (!seen.add(file.getName())) {
            return;
        }
        for (Descriptors.FileDescriptor dependency : file.getDependencies()) {
            collectFiles(dependency, seen, set);
        }
        set.addFile(file.toProto());
    }

    /** Runs the action, naming the spec entry in the message of the error it throws. */
    private static <T> T withContext(String context, Supplier<T> action) {
        try {
            return action.get();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(context + ": " + e.getMessage(), e);
        }
    }

    /** The string under {@code key}, unless it is absent or not a string. */
    private static Optional<String> stringValue(YTreeMapNode parent, String key) {
        return parent.get(key).filter(YTreeNode::isStringNode).map(YTreeNode::stringValue);
    }

    /**
     * The string under {@code key}, unless it is absent or empty; a node of another type fails,
     * since overwriting it would submit a different spec than the one written.
     */
    private static Optional<String> nonEmptyString(YTreeMapNode parent, String key) {
        return parent.get(key).flatMap(existing -> {
            if (!existing.isStringNode()) {
                throw new IllegalArgumentException(
                        "The \"%s\" node must be a string, got: %s".formatted(key, existing));
            }
            return existing.bytesValue().length > 0 ? Optional.of(existing.stringValue()) : Optional.empty();
        });
    }
}

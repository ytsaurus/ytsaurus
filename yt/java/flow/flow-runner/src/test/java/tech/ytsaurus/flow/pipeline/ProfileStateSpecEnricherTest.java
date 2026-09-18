package tech.ytsaurus.flow.pipeline;

import java.util.List;
import java.util.Map;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.flow.state.StateDescriptor;
import tech.ytsaurus.flow.state.StateDescriptors;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProfileStateSpecEnricherTest {

    private static final String MANAGERS = "external_state_managers";
    private static final String JOINERS = "external_state_joiners";
    private static final String MANAGER_CLASS_KEY = "external_state_manager_class_name";
    private static final String JOINER_CLASS_KEY = "external_state_joiner_class_name";
    private static final String PATH_ONLY = "{ \"path\" = \"//tmp/state\" }";

    private static final StateDescriptor<?> STATE = StateDescriptors.externalProto("/state", Timestamp.class);

    /** One computation entry: a state "/state" of the given section, class key, class and parameters. */
    private static String profileState(
            String computation, String section, String classKey, String className, String parametersYson
    ) {
        return """
                "%s" = { "%s" = { "/state" = { "%s" = "%s"; "parameters" = %s; }; }; };
                """.formatted(computation, section, classKey, className, parametersYson);
    }

    private static YTreeMapNode specOf(String... computations) {
        return parse("{ \"computations\" = { %s } }".formatted(String.join("", computations)));
    }

    /** A spec with one profile state manager "/state" of computation "join" and the given parameters. */
    private static YTreeMapNode profileSpec(String managerClass, String parametersYson) {
        return specOf(profileState("join", MANAGERS, MANAGER_CLASS_KEY, managerClass, parametersYson));
    }

    private static YTreeMapNode descriptorLayoutOf(YTreeMapNode spec) {
        return descriptorLayoutOf(spec, "join", MANAGERS);
    }

    private static YTreeMapNode descriptorLayoutOf(YTreeMapNode spec, String computation, String section) {
        return spec.getMap("computations").getMap(computation).getMap(section)
                .getMap("/state").getMap("parameters").getMap("descriptor_layout");
    }

    private static List<String> filesOf(YTreeMapNode descriptorLayout) throws Exception {
        return DescriptorProtos.FileDescriptorSet.parseFrom(descriptorLayout.getBytes("descriptor_set"))
                .getFileList().stream().map(DescriptorProtos.FileDescriptorProto::getName).toList();
    }

    private static YTreeMapNode parse(String yson) {
        return YTreeTextSerializer.deserialize(yson).mapNode();
    }

    private static IllegalArgumentException patchFails(YTreeMapNode spec, List<StateDescriptor<?>> states) {
        return assertThrows(IllegalArgumentException.class, () -> ProfileStateSpecEnricher.patch(spec, states));
    }

    @Test
    void testDescribesProfileStateFromDeclaredMessage() throws Exception {
        YTreeMapNode spec = profileSpec(ProfileStateSpecEnricher.PROFILE_STATE_MANAGER_CLASS, PATH_ONLY);

        ProfileStateSpecEnricher.patch(spec, List.of(STATE));

        YTreeMapNode descriptorLayout = descriptorLayoutOf(spec);
        assertEquals("google.protobuf.Timestamp", descriptorLayout.getString("message_name"));
        assertEquals(List.of("google/protobuf/timestamp.proto"), filesOf(descriptorLayout));
    }

    @Test
    void testKeepsDeclaredOptionsAndMatchingMessageName() {
        YTreeMapNode spec = profileSpec(ProfileStateSpecEnricher.PROFILE_STATE_MANAGER_CLASS, """
                {
                    "path" = "//tmp/state";
                    "descriptor_layout" = {
                        "message_name" = "google.protobuf.Timestamp";
                        "name_policy" = "snake_case";
                    };
                }
                """);

        ProfileStateSpecEnricher.patch(spec, List.of(STATE));

        YTreeMapNode patched = descriptorLayoutOf(spec);
        assertEquals("snake_case", patched.getString("name_policy"));
        assertTrue(patched.getBytes("descriptor_set").length > 0);
    }

    @Test
    void testKeepsExplicitDescriptorSet() {
        YTreeMapNode spec = profileSpec(ProfileStateSpecEnricher.PROFILE_STATE_MANAGER_CLASS, """
                { "descriptor_layout" = { "message_name" = "x.Y"; "descriptor_set" = "\\x01\\x02\\x03" } }
                """);

        ProfileStateSpecEnricher.patch(spec, List.of(STATE));

        assertEquals("x.Y", descriptorLayoutOf(spec).getString("message_name"));
        assertArrayEquals(new byte[] {1, 2, 3}, descriptorLayoutOf(spec).getBytes("descriptor_set"));
    }

    @Test
    void testKeepsExplicitLayout() {
        YTreeMapNode spec = profileSpec(ProfileStateSpecEnricher.PROFILE_STATE_MANAGER_CLASS, """
                { "path" = "//tmp/state"; "layout" = "serialized-layout" }
                """);
        String before = YTreeTextSerializer.serialize(spec);

        ProfileStateSpecEnricher.patch(spec, List.of(STATE));

        assertEquals(before, YTreeTextSerializer.serialize(spec));
    }

    @Test
    void testTreatsEmptyLayoutAndDescriptorSetAsAbsent() throws Exception {
        // The C++ parser takes an empty string for an absent layout; specs spell the default that way.
        YTreeMapNode spec = profileSpec(ProfileStateSpecEnricher.PROFILE_STATE_MANAGER_CLASS, """
                {
                    "path" = "//tmp/state";
                    "layout" = "";
                    "descriptor_layout" = { "descriptor_set" = ""; "name_policy" = "snake_case" };
                }
                """);

        ProfileStateSpecEnricher.patch(spec, List.of(STATE));

        YTreeMapNode patched = descriptorLayoutOf(spec);
        assertEquals("google.protobuf.Timestamp", patched.getString("message_name"));
        assertEquals(List.of("google/protobuf/timestamp.proto"), filesOf(patched));
        assertEquals("snake_case", patched.getString("name_policy"));
    }

    @Test
    void testKeepsMessageNameOfUndeclaredState() {
        // No declared state to describe it from: the message is compiled into a custom flow_server.
        YTreeMapNode spec = profileSpec(ProfileStateSpecEnricher.PROFILE_STATE_MANAGER_CLASS, """
                { "path" = "//tmp/state"; "descriptor_layout" = { "message_name" = "x.Y" } }
                """);
        String before = YTreeTextSerializer.serialize(spec);

        ProfileStateSpecEnricher.patch(spec, List.of());

        assertEquals(before, YTreeTextSerializer.serialize(spec));
    }

    @Test
    void testHonorsClassNameAlias() {
        YTreeMapNode spec = specOf(profileState(
                "join", MANAGERS, "class_name", ProfileStateSpecEnricher.PROFILE_STATE_MANAGER_CLASS, PATH_ONLY));

        ProfileStateSpecEnricher.patch(spec, List.of(STATE));

        assertEquals("google.protobuf.Timestamp", descriptorLayoutOf(spec).getString("message_name"));
    }

    @Test
    void testDescribesOwnerAndJoinerDeclaredUnderOneName() throws Exception {
        // The owner and the joining computation declare the state under one name, each with its own
        // descriptor; both spec entries are described from the one message.
        YTreeMapNode spec = specOf(
                profileState(
                        "owner", MANAGERS, MANAGER_CLASS_KEY, ProfileStateSpecEnricher.PROFILE_STATE_MANAGER_CLASS,
                        PATH_ONLY),
                profileState(
                        "reader", JOINERS, JOINER_CLASS_KEY, ProfileStateSpecEnricher.PROFILE_STATE_JOINER_CLASS,
                        PATH_ONLY));
        List<StateDescriptor<?>> declared = List.of(
                STATE, StateDescriptors.externalProtoReadOnly("/state", Timestamp.class));

        ProfileStateSpecEnricher.patch(spec, declared);

        for (var entry : Map.of("owner", MANAGERS, "reader", JOINERS).entrySet()) {
            YTreeMapNode patched = descriptorLayoutOf(spec, entry.getKey(), entry.getValue());
            assertEquals("google.protobuf.Timestamp", patched.getString("message_name"));
            assertEquals(List.of("google/protobuf/timestamp.proto"), filesOf(patched));
        }
    }

    @Test
    void testRejectsSeveralMessagesUnderOneName() {
        YTreeMapNode spec = profileSpec(ProfileStateSpecEnricher.PROFILE_STATE_MANAGER_CLASS, PATH_ONLY);

        IllegalArgumentException error = patchFails(spec, List.of(
                STATE, StateDescriptors.externalProto("/state", Duration.class)));
        assertTrue(error.getMessage().contains("google.protobuf.Timestamp"));
        assertTrue(error.getMessage().contains("google.protobuf.Duration"));
        assertTrue(error.getMessage().contains("different names"));
    }

    @Test
    void testIgnoresOtherManagerClasses() {
        YTreeMapNode spec = profileSpec("NYT::NFlow::TSimpleExternalStateManager", PATH_ONLY);
        String before = YTreeTextSerializer.serialize(spec);

        ProfileStateSpecEnricher.patch(spec, List.of());

        assertEquals(before, YTreeTextSerializer.serialize(spec));
    }

    @Test
    void testRejectsUndeclaredProfileState() {
        YTreeMapNode spec = profileSpec(ProfileStateSpecEnricher.PROFILE_STATE_MANAGER_CLASS, PATH_ONLY);

        IllegalArgumentException error = patchFails(spec, List.of());
        assertTrue(error.getMessage().contains("Profile state /state of computation join"));
        assertTrue(error.getMessage().contains("StateDescriptors.externalProto(...)"));
    }

    @Test
    void testUndeclaredJoinerHintsAtTheReadOnlyFactory() {
        YTreeMapNode spec = specOf(profileState(
                "reader", JOINERS, JOINER_CLASS_KEY, ProfileStateSpecEnricher.PROFILE_STATE_JOINER_CLASS, PATH_ONLY));

        IllegalArgumentException error = patchFails(spec, List.of());
        assertTrue(error.getMessage().contains("StateDescriptors.externalProtoReadOnly(...)"));
    }

    @Test
    void testUndeclaredStateNamesWhatIsDeclaredInstead() {
        YTreeMapNode spec = profileSpec(ProfileStateSpecEnricher.PROFILE_STATE_MANAGER_CLASS, PATH_ONLY);

        // Declared with a descriptor that has no message.
        IllegalArgumentException elsewhere = patchFails(spec, List.of(StateDescriptors.external("/state")));
        assertTrue(elsewhere.getMessage().contains("ExternalStateDescriptor, which has no message"));

        // Declared under another name: a typo in the spec key or the descriptor name.
        IllegalArgumentException misnamed = patchFails(spec, List.of(
                StateDescriptors.externalProto("/join_state", Timestamp.class)));
        assertTrue(misnamed.getMessage().contains("the declared proto states are /join_state"));
    }

    @Test
    void testLeavesMessageNameOfAnotherComputationsStateToTheWorkerPool() {
        // A Java computation declares "/state"; a C++ computation has its own "/state" with a message
        // compiled into a custom flow_server, so the spec is left as written.
        YTreeMapNode spec = profileSpec(ProfileStateSpecEnricher.PROFILE_STATE_MANAGER_CLASS, """
                { "path" = "//tmp/state"; "descriptor_layout" = { "message_name" = "cpp.B" } }
                """);
        String before = YTreeTextSerializer.serialize(spec);

        ProfileStateSpecEnricher.patch(spec, List.of(STATE));

        assertEquals(before, YTreeTextSerializer.serialize(spec));
    }

    @Test
    void testRejectsMalformedNodesNamingTheState() {
        // A wrong node type is never repaired: the spec would differ from the one written.
        for (String parameters : List.of(
                "{ \"path\" = \"//tmp/state\"; \"descriptor_layout\" = # }",
                "{ \"path\" = \"//tmp/state\"; \"descriptor_layout\" = { \"message_name\" = 123 } }",
                "{ \"path\" = \"//tmp/state\"; \"layout\" = 5 }")) {
            YTreeMapNode spec = profileSpec(ProfileStateSpecEnricher.PROFILE_STATE_MANAGER_CLASS, parameters);
            String before = YTreeTextSerializer.serialize(spec);

            IllegalArgumentException error = patchFails(spec, List.of(STATE));
            assertTrue(error.getMessage().contains("Profile state /state of computation join"), parameters);
            assertEquals(before, YTreeTextSerializer.serialize(spec));
        }
    }
}

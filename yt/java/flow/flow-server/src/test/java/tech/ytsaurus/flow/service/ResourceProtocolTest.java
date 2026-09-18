package tech.ytsaurus.flow.service;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.flow.internal.resource.CompanionResourceInstanceReference;
import tech.ytsaurus.flow.rpc.EResourceExecuteStatus;
import tech.ytsaurus.ysontree.YTree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResourceProtocolTest {
    private static final GUID A = GUID.valueOf("1-2-3-4");
    private static final GUID B = GUID.valueOf("5-6-7-8");

    @ParameterizedTest(name = "{0}")
    @MethodSource("decisions")
    void initDecision(
            String name, ResourceProtocol history, InitCommandArg incoming, boolean ready,
            Class<?> decisionType, EResourceExecuteStatus status
    ) {
        var decision = history.decideInit("r", incoming, incoming.canonicalSpecs(), ready);
        assertInstanceOf(decisionType, decision, name);
        if (decision instanceof ResourceProtocol.Reply reply) {
            assertEquals(status, reply.outcome().status(), name);
        }
    }

    static Stream<Arguments> decisions() {
        var initial = arg(A, 1, 2, "s", "d", List.of());
        var known = new ResourceProtocol(new ResourceProtocol.Incarnation(A, 1, false), null);
        var applied = known.applied(initial, initial.canonicalSpecs());
        var dependency = List.of(new CompanionResourceInstanceReference("dep", B, 1, "alias"));
        var changedRevision = new InitCommandArg(initial.spec(), initial.dynamicSpec(), A, 1, 2,
                List.of(), YTree.stringNode("revision"));
        return Stream.of(
                Arguments.of("first init", ResourceProtocol.EMPTY, initial, false,
                        ResourceProtocol.Create.class, EResourceExecuteStatus.RES_OK),
                Arguments.of("duplicate init", applied, initial, true,
                        ResourceProtocol.Reply.class, EResourceExecuteStatus.RES_OK),
                Arguments.of("new incarnation", applied, arg(B, 2, 0, "s2", "d2", List.of()), true,
                        ResourceProtocol.Create.class, EResourceExecuteStatus.RES_OK),
                Arguments.of("old incarnation", applied, arg(B, 0, 9, "s", "d", List.of()), true,
                        ResourceProtocol.Reply.class, EResourceExecuteStatus.RES_STALE_RESOURCE_INCARNATION),
                Arguments.of("conflicting incarnation id", applied, arg(B, 1, 2, "s", "d", List.of()), true,
                        ResourceProtocol.Reply.class, EResourceExecuteStatus.RES_STALE_RESOURCE_INCARNATION),
                Arguments.of("retired id stays retired", applied.unload(A), arg(A, 2, 3, "s", "d", List.of()), false,
                        ResourceProtocol.Reply.class, EResourceExecuteStatus.RES_STALE_RESOURCE_INCARNATION),
                Arguments.of("tombstone allows successor", ResourceProtocol.EMPTY.unload(A),
                        arg(B, 2, 0, "s", "d", List.of()), false,
                        ResourceProtocol.Create.class, EResourceExecuteStatus.RES_OK),
                Arguments.of("static conflict", applied, arg(A, 1, 3, "s2", "d", List.of()), true,
                        ResourceProtocol.Reply.class, EResourceExecuteStatus.RES_ERROR),
                Arguments.of("dynamic conflict", applied, arg(A, 1, 2, "s", "d2", List.of()), true,
                        ResourceProtocol.Reply.class, EResourceExecuteStatus.RES_ERROR),
                Arguments.of("revision conflict", applied, changedRevision, true,
                        ResourceProtocol.Reply.class, EResourceExecuteStatus.RES_ERROR),
                Arguments.of("dependency changed at same generation", applied, arg(A, 1, 2, "s", "d", dependency), true,
                        ResourceProtocol.Create.class, EResourceExecuteStatus.RES_OK),
                Arguments.of("dependency changed at new generation", applied, arg(A, 1, 3, "s", "d2", dependency), true,
                        ResourceProtocol.Create.class, EResourceExecuteStatus.RES_OK),
                Arguments.of("rebuild after failure", applied, initial, false,
                        ResourceProtocol.Create.class, EResourceExecuteStatus.RES_OK),
                Arguments.of("old config ignores payload changes", applied, arg(A, 1, 1, "other", "other", List.of()),
                        true, ResourceProtocol.Reply.class, EResourceExecuteStatus.RES_OK),
                Arguments.of("old config cannot revive failed object", applied, arg(A, 1, 1, "s", "d", List.of()),
                        false, ResourceProtocol.Reply.class, EResourceExecuteStatus.RES_RESOURCE_NOT_INITIALIZED),
                Arguments.of("new dynamic config reuses object", applied, arg(A, 1, 3, "s", "d2", List.of()), true,
                        ResourceProtocol.Reconfigure.class, EResourceExecuteStatus.RES_OK),
                Arguments.of("new config without object rebuilds", applied, arg(A, 1, 3, "s", "d2", List.of()), false,
                        ResourceProtocol.Create.class, EResourceExecuteStatus.RES_OK),
                Arguments.of("failed initial payload is not binding", known, arg(A, 1, 0, "corrected", "d", List.of()),
                        false, ResourceProtocol.Create.class, EResourceExecuteStatus.RES_OK),
                Arguments.of("unavailable instance still rejects static conflict", applied,
                        arg(A, 1, 3, "s2", "d", List.of()), false,
                        ResourceProtocol.Reply.class, EResourceExecuteStatus.RES_ERROR),
                Arguments.of("unavailable instance still rejects dynamic conflict", applied,
                        arg(A, 1, 2, "s", "d2", List.of()), false,
                        ResourceProtocol.Reply.class, EResourceExecuteStatus.RES_ERROR),
                Arguments.of("unavailable instance still rejects revision conflict", applied, changedRevision, false,
                        ResourceProtocol.Reply.class, EResourceExecuteStatus.RES_ERROR),
                Arguments.of("unchanged payload at a newer generation still reconfigures", applied,
                        arg(A, 1, 3, "s", "d", List.of()), true,
                        ResourceProtocol.Reconfigure.class, EResourceExecuteStatus.RES_OK),
                Arguments.of("new incarnation can reuse an active id", applied, arg(A, 2, 0, "s2", "d2", dependency),
                        true, ResourceProtocol.Create.class, EResourceExecuteStatus.RES_OK)
        );
    }

    @Test
    void unloadRetainsOnlyTheIncarnationFenceAndIsIdempotent() {
        var arg = arg(A, 1, 2, "s", "d", List.of());
        var history = new ResourceProtocol(new ResourceProtocol.Incarnation(A, 1, false), null)
                .applied(arg, arg.canonicalSpecs());
        assertSame(history, history.unload(B));
        var retired = history.unload(A);
        assertTrue(retired.incarnation().retired());
        assertEquals(1, retired.incarnation().generation());
        assertNull(retired.applied());
        assertSame(retired, retired.unload(A));
    }

    @Test
    void replacementFencesOlderCommandsBeforeLoadSucceeds() {
        var arg = arg(A, 3, 2, "s", "d", List.of());
        var create = assertInstanceOf(ResourceProtocol.Create.class,
                ResourceProtocol.EMPTY.decideInit("r", arg, arg.canonicalSpecs(), false));
        assertEquals(A, create.history().incarnation().id());
        assertNull(create.history().applied());
        var old = arg(B, 2, 9, "s", "d", List.of());
        var reply = assertInstanceOf(ResourceProtocol.Reply.class,
                create.history().decideInit("r", old, old.canonicalSpecs(), false));
        assertEquals(EResourceExecuteStatus.RES_STALE_RESOURCE_INCARNATION, reply.outcome().status());
    }

    @Test
    void rebuildingRetainsAppliedHistoryButANewerIncarnationStartsWithoutIt() {
        var initial = arg(A, 1, 2, "s", "d", List.of());
        var history = new ResourceProtocol(new ResourceProtocol.Incarnation(A, 1, false), null)
                .applied(initial, initial.canonicalSpecs());

        var rebuild = assertInstanceOf(ResourceProtocol.Create.class,
                history.decideInit("r", initial, initial.canonicalSpecs(), false));
        assertSame(history, rebuild.history());

        var successor = arg(A, 2, 0, "new static spec", "new dynamic spec", List.of());
        var replacement = assertInstanceOf(ResourceProtocol.Create.class,
                history.decideInit("r", successor, successor.canonicalSpecs(), true));
        assertEquals(new ResourceProtocol.Incarnation(A, 2, false), replacement.history().incarnation());
        assertNull(replacement.history().applied());
    }

    @Test
    void unloadBeforeInitRemembersTheRetiredIdWithoutFencingOtherIds() {
        var retired = ResourceProtocol.EMPTY.unload(A);
        assertEquals(new ResourceProtocol.Incarnation(A, 0, true), retired.incarnation());
        assertNull(retired.applied());
        assertSame(retired, retired.unload(A));
        assertSame(retired, retired.unload(B));

        var lateInit = arg(A, 2, 0, "s", "d", List.of());
        var rejected = assertInstanceOf(ResourceProtocol.Reply.class,
                retired.decideInit("r", lateInit, lateInit.canonicalSpecs(), false));
        assertEquals(EResourceExecuteStatus.RES_STALE_RESOURCE_INCARNATION, rejected.outcome().status());

        var successor = arg(B, 2, 0, "s", "d", List.of());
        var accepted = assertInstanceOf(ResourceProtocol.Create.class,
                retired.decideInit("r", successor, successor.canonicalSpecs(), false));
        assertEquals(new ResourceProtocol.Incarnation(B, 2, false), accepted.history().incarnation());
    }

    private static InitCommandArg arg(
            GUID id, long incarnation, long generation, String spec, String dynamic,
            List<CompanionResourceInstanceReference> dependencies
    ) {
        return new InitCommandArg(YTree.stringNode(spec), YTree.stringNode(dynamic), id,
                incarnation, generation, dependencies, null);
    }
}

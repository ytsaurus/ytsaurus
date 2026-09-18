package tech.ytsaurus.flow.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LifecycleGateTest {

    @Test
    @DisplayName("A command admitted before closure is answered for until it leaves")
    void commandAdmittedBeforeClosureIsAnsweredFor() {
        var gate = new LifecycleGate();
        assertTrue(gate.tryEnterCommand());

        gate.close();
        assertTrue(gate.isClosed());
        assertFalse(gate.isQuiescent());

        gate.leaveCommand();
        assertTrue(gate.isQuiescent());
    }

    @Test
    @DisplayName("A command arriving after closure is refused and leaves nothing to answer for")
    void commandArrivingAfterClosureIsRefused() {
        var gate = new LifecycleGate();
        gate.close();

        assertFalse(gate.tryEnterCommand());
        assertTrue(gate.isQuiescent());
    }

    @Test
    @DisplayName("An instance built inside a command keeps the gate busy across the handoff")
    void instanceHandoffNeverExposesQuiescence() {
        var gate = new LifecycleGate();
        assertTrue(gate.tryEnterCommand());
        gate.close();

        gate.instanceCreated();
        gate.leaveCommand();
        assertFalse(gate.isQuiescent());

        gate.instanceDropped();
        assertTrue(gate.isQuiescent());
    }

    @Test
    @DisplayName("An open gate is never quiescent")
    void openGateIsNeverQuiescent() {
        var gate = new LifecycleGate();
        assertFalse(gate.isQuiescent());
    }
}

package tech.ytsaurus.flow.service;

/**
 * Admission and quiescence of one resource runtime. Also the short metadata lock used by its store
 * and instances; user hooks never run under this monitor.
 */
final class LifecycleGate {

    private volatile boolean closed;
    private int activeCommands;
    private int liveInstances;

    /**
     * Admits a lifecycle command unless the store is closed; an admitted command must
     * {@link #leaveCommand()} once it has returned.
     */
    synchronized boolean tryEnterCommand() {
        if (closed) {
            return false;
        }
        ++activeCommands;
        return true;
    }

    synchronized void leaveCommand() {
        --activeCommands;
    }

    synchronized void close() {
        closed = true;
    }

    boolean isClosed() {
        return closed;
    }

    /**
     * Counts an instance in; it is counted out by {@link #instanceDropped()} once its unload hook
     * has run. An instance is created inside the command that builds it, so the two counts never
     * both read zero while it is pending.
     */
    synchronized void instanceCreated() {
        ++liveInstances;
    }

    synchronized void instanceDropped() {
        --liveInstances;
    }

    /**
     * Whether the store is closed with no command inside user code and no instance whose unload
     * hook is still to run. Final once true: nothing is admitted after closure, and the counts
     * only fall.
     */
    synchronized boolean isQuiescent() {
        return closed && activeCommands == 0 && liveInstances == 0;
    }
}

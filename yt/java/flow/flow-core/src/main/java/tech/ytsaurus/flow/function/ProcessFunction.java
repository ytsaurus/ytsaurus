package tech.ytsaurus.flow.function;

/**
 * Sealed marker interface for all process functions.
 *
 * @param <Key> key type for stateful operators
 */
public sealed interface ProcessFunction<Key> extends FlowFunction
        permits RowFunction, BatchFunction {
}

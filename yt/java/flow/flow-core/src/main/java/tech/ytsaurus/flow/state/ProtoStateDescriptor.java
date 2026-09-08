package tech.ytsaurus.flow.state;

import com.google.protobuf.Descriptors;

/**
 * A {@link StateDescriptor} whose state is a protobuf message.
 *
 * <p>Exposes the message descriptor so the runner can describe the state to the worker: the profile
 * state manager derives its table layout from the annotated message, and the worker never compiles
 * the user's proto, so the descriptors travel in the pipeline spec.
 */
public interface ProtoStateDescriptor {
    /**
     * Returns the descriptor of the state message.
     *
     * @return the message descriptor
     */
    Descriptors.Descriptor getMessageDescriptor();
}

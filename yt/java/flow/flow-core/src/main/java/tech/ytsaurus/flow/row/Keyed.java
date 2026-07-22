package tech.ytsaurus.flow.row;

/**
 * Common abstraction for entities that expose a key.
 * <p>
 * Implemented by {@link ExtendedMessage} and {@link Timer}, which carry a key alongside
 * their payload.
 */
public interface Keyed {
    /**
     * Returns the {@link Payload} representing the key of this entity.
     *
     * @return key.
     */
    Payload getKey();
}

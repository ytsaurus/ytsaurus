package tech.ytsaurus.flow.row.codec;

/**
 * Extension point for installing a non-default {@link CodecRegistry} into the running JVM.
 *
 * <p>Discovered exactly once via {@link java.util.ServiceLoader} when
 * {@link CodecRegistry#getInstance()} is first invoked. The discovered provider's
 * {@link #getCodecRegistry()} return value becomes the JVM-wide active registry.
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li>A provider returns a single {@link CodecRegistry} describing a whole codec
 *       profile (key, internal-state value, payload, typed-payload and YSON codecs);
 *       it does not describe individual codecs in isolation.</li>
 *   <li>Implementations must be stateless and thread-safe.</li>
 *   <li>{@link #getCodecRegistry()} must not return {@code null}.</li>
 *   <li>At most one provider may be visible on the classpath in a production JVM.
 *       If more than one is found, {@link CodecRegistry#getInstance()} fails fast
 *       with {@link IllegalStateException}.</li>
 *   <li>If no provider is found, {@link CodecRegistry#getInstance()} falls back to
 *       {@link CodecRegistry#DEFAULT}.</li>
 * </ul>
 *
 * <p>To register an implementation place its fully-qualified class name on a line of
 * the {@code META-INF/services/tech.ytsaurus.flow.row.codec.CodecRegistryProvider}
 * service descriptor inside the module that wants to install codecs.
 */
public interface CodecRegistryProvider {

    /**
     * Returns the codec registry to install JVM-wide.
     *
     * @return a non-null codec registry
     */
    CodecRegistry getCodecRegistry();
}

package tech.ytsaurus.flow.row.codec;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;


/**
 * Immutable bundle of the pluggable codecs used by the Flow request/response pipeline:
 * key, internal-state value, payload, typed-payload and YSON codecs.
 *
 * <p>{@link #getInstance()} returns the JVM-wide active registry. It is initialized
 * lazily on first invocation via {@link ServiceLoader} discovery of
 * {@link CodecRegistryProvider}; see that interface for the discovery rules. There is no
 * production-supported way to install a registry other than placing a single
 * {@code CodecRegistryProvider} on the classpath.
 */
public final class CodecRegistry {

    /**
     * Shared registry instance pre-populated with codecs that produce the legacy
     * wire-protocol-compatible encoding.
     */
    public static final CodecRegistry DEFAULT = new CodecRegistry(
            ProtoByteStringKeyCodec.INSTANCE,
            IdentityInternalStateValueCodec.INSTANCE,
            ProtoWirePayloadCodec.INSTANCE,
            TypeAwareProtoWireTypedPayloadCodec.INSTANCE,
            DefaultYsonCodec.INSTANCE
    );

    private static final Object INSTANCE_LOCK = new Object();
    private static volatile @Nullable CodecRegistry instance;

    private final KeyCodec keyCodec;
    private final InternalStateValueCodec internalStateValueCodec;
    private final PayloadCodec payloadCodec;
    private final TypedPayloadCodec typedPayloadCodec;
    private final YsonCodec ysonCodec;

    /**
     * Creates a registry with the supplied codecs.
     *
     * @param keyCodec                codec for message/timer/state keys
     * @param internalStateValueCodec codec for opaque internal-state value blobs
     * @param payloadCodec            factory of codecs for raw payload blobs
     * @param typedPayloadCodec       factory of codecs for typed-stream payloads
     * @param ysonCodec               codec for YSON-encoded {@code STRING}/{@code ANY}/
     *                                {@code COMPOSITE} values
     */
    public CodecRegistry(
            KeyCodec keyCodec,
            InternalStateValueCodec internalStateValueCodec,
            PayloadCodec payloadCodec,
            TypedPayloadCodec typedPayloadCodec,
            YsonCodec ysonCodec
    ) {
        this.keyCodec = Objects.requireNonNull(keyCodec, "keyCodec must not be null");
        this.internalStateValueCodec = Objects.requireNonNull(
                internalStateValueCodec, "internalStateValueCodec must not be null"
        );
        this.payloadCodec = Objects.requireNonNull(payloadCodec, "payloadCodec must not be null");
        this.typedPayloadCodec = Objects.requireNonNull(typedPayloadCodec, "typedPayloadCodec must not be null");
        this.ysonCodec = Objects.requireNonNull(ysonCodec, "ysonCodec must not be null");
    }

    /**
     * Returns the JVM-wide active {@link CodecRegistry}.
     *
     * <p>On first invocation the registry is resolved as follows:
     * <ol>
     *   <li>Providers are discovered via
     *       {@code ServiceLoader.load(CodecRegistryProvider.class)}.</li>
     *   <li>If no provider is found, {@link #DEFAULT} is used.</li>
     *   <li>If exactly one provider is found, its {@link CodecRegistryProvider#getCodecRegistry()}
     *       result is used (and must be non-null).</li>
     *   <li>If more than one provider is found, an {@link IllegalStateException} is thrown
     *       listing all discovered provider classes; this is intentionally fail-fast because
     *       silently picking one would hide a misconfigured classpath.</li>
     * </ol>
     *
     * <p>The resolved instance is cached for the lifetime of the JVM.
     *
     * @return the active codec registry; never {@code null}
     */
    public static CodecRegistry getInstance() {
        var local = instance;
        if (local != null) {
            return local;
        }
        synchronized (INSTANCE_LOCK) {
            if (instance == null) {
                var providers = new ArrayList<CodecRegistryProvider>();
                for (var provider : ServiceLoader.load(CodecRegistryProvider.class)) {
                    providers.add(provider);
                }
                instance = resolveFromProviders(providers);
            }
            return instance;
        }
    }

    /**
     * Resolves an active registry from a snapshot of discovered providers. Visible for
     * test-only access via {@code CodecRegistryTestSupport}.
     */
    static CodecRegistry resolveFromProviders(List<CodecRegistryProvider> providers) {
        if (providers.isEmpty()) {
            return DEFAULT;
        }
        if (providers.size() > 1) {
            String names = providers.stream()
                    .map(p -> p.getClass().getName())
                    .collect(Collectors.joining(", "));
            throw new IllegalStateException(
                    "Multiple CodecRegistryProvider implementations found on the classpath: [%s]. "
                            .formatted(names)
                            + "At most one provider is supported in a production JVM."
            );
        }
        var provider = providers.getFirst();
        var registry = provider.getCodecRegistry();
        if (registry == null) {
            throw new IllegalStateException(
                    "CodecRegistryProvider %s returned a null CodecRegistry"
                            .formatted(provider.getClass().getName())
            );
        }
        return registry;
    }

    /**
     * Returns the codec used for (de)serializing message, timer and state keys.
     *
     * @return the registered key codec
     */
    public KeyCodec getKeyCodec() {
        return keyCodec;
    }

    /**
     * Returns the codec used for (de)serializing opaque internal-state value blobs.
     *
     * @return the registered internal-state value codec
     */
    public InternalStateValueCodec getInternalStateValueCodec() {
        return internalStateValueCodec;
    }

    /**
     * Returns the codec used for (de)serializing {@link tech.ytsaurus.flow.row.Payload} blobs
     * (both raw-stream payloads and external-state values).
     *
     * @return the registered payload codec
     */
    public PayloadCodec getPayloadCodec() {
        return payloadCodec;
    }

    /**
     * Returns the codec used for (de)serializing typed-stream payloads.
     *
     * @return the registered typed-payload codec
     */
    public TypedPayloadCodec getTypedPayloadCodec() {
        return typedPayloadCodec;
    }

    /**
     * Returns the codec used for YSON-encoded {@code STRING}/{@code ANY}/{@code COMPOSITE}
     * column values.
     *
     * @return the registered YSON codec
     */
    public YsonCodec getYsonCodec() {
        return ysonCodec;
    }
}

package tech.ytsaurus.flow.state;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.jspecify.annotations.Nullable;

/**
 * External state entry in the {@link StateFormat#PROTO} wire format.
 *
 * <p>{@code serialized} carries the payload bytes as received from the worker; {@code message}
 * is a parsed message written through {@link ProtoStateAccessor#set} — when present, it is the
 * source of truth and is re-serialized into the response.
 *
 * <p>The inherited {@link ExternalState} payload value is always {@code null}: proto states have
 * no row schema.
 */
public final class ProtoExternalState extends ExternalState {

    public static final ProtoExternalState RESET = new ProtoExternalState();

    private final @Nullable ByteString serialized;
    private final @Nullable Message message;
    // Lazy parse of |serialized|, filled by the accessor on first read. The
    // entry is request-local and single-threaded; set()/clear() replace the
    // whole entry, so the cache never outlives its bytes.
    private @Nullable Message parsed;

    private ProtoExternalState() {
        super(true, null);
        this.serialized = null;
        this.message = null;
    }

    /**
     * Creates an entry from received payload bytes.
     *
     * @param serialized serialized message bytes; may be empty for an all-default message
     */
    public ProtoExternalState(ByteString serialized) {
        super(false, null);
        this.serialized = serialized;
        this.message = null;
    }

    /**
     * Creates an entry from received payload bytes; the array is copied, so later mutations of
     * the caller's array cannot desync the entry from its cached parse.
     *
     * @param serialized serialized message bytes; may be empty for an all-default message
     */
    public ProtoExternalState(byte[] serialized) {
        this(ByteString.copyFrom(serialized));
    }

    /**
     * Creates an entry from a message written by user code.
     *
     * @param message the state message
     */
    public ProtoExternalState(Message message) {
        super(false, null);
        this.serialized = null;
        this.message = message;
    }

    /**
     * Returns the message written by user code, or {@code null} when this entry carries received
     * bytes.
     */
    public @Nullable Message getMessage() {
        return message;
    }

    /**
     * Returns the cached parse of the received bytes, or {@code null} when nothing was cached.
     */
    @Nullable Message getParsed() {
        return parsed;
    }

    /**
     * Caches the parse of the received bytes so repeated reads of the same entry skip protobuf
     * parsing. Protobuf messages are immutable, so sharing one instance across reads is safe.
     */
    void setParsed(Message parsed) {
        this.parsed = parsed;
    }

    /**
     * Serializes this entry's payload: the written message when present, otherwise the received
     * bytes. An all-default message legitimately serializes to zero bytes. The returned value
     * is immutable, so callers cannot desync the entry from its cached parse.
     *
     * @return serialized payload bytes
     * @throws IllegalStateException if this entry is a reset marker
     */
    public ByteString serialize() {
        if (isReset()) {
            throw new IllegalStateException("Cannot serialize a reset proto state entry");
        }
        if (message != null) {
            return message.toByteString();
        }
        return serialized != null ? serialized : ByteString.EMPTY;
    }
}

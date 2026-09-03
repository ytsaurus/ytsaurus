package tech.ytsaurus.flow.state;

import java.util.Objects;

import com.google.protobuf.ByteString;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.codec.ByteStringCodec;

/**
 * External state entry. Like an internal state it stores the value as undecoded wire bytes; the
 * accessors decode it against the state schema on access.
 */
public class ExternalState extends State<ByteString> {

    public static final ExternalState RESET = new ExternalState(true, null);

    // Lazy decode of the stored bytes, filled by the accessor on first read. The entry is
    // request-local and single-threaded; set()/clear() replace the whole entry, so the cache
    // never outlives its bytes.
    private @Nullable Payload decoded;

    public ExternalState(ByteString value) {
        super(value);
    }

    public ExternalState(boolean reset, @Nullable ByteString value) {
        super(reset, value);
    }

    /**
     * Returns the stored value decoded with {@code codec}, decoding on the first call.
     *
     * @param codec codec bound to the state schema.
     * @return decoded payload.
     */
    Payload decode(ByteStringCodec<Payload> codec) {
        if (decoded == null) {
            decoded = codec.decode(Objects.requireNonNull(getValue(), "Non-reset state must have value"));
        }
        return decoded;
    }
}

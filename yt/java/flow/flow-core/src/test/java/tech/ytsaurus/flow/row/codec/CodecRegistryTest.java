package tech.ytsaurus.flow.row.codec;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;


public class CodecRegistryTest {

    @Test
    public void defaultRegistryUsesLegacyCodecs() {
        var registry = CodecRegistry.DEFAULT;
        assertSame(ProtoByteStringKeyCodec.INSTANCE, registry.getKeyCodec());
        assertSame(IdentityInternalStateValueCodec.INSTANCE, registry.getInternalStateValueCodec());
        assertSame(ProtoWirePayloadCodec.INSTANCE, registry.getPayloadCodec());
        assertSame(TypeAwareProtoWireTypedPayloadCodec.INSTANCE, registry.getTypedPayloadCodec());
    }

    @Test
    public void defaultRegistryUsesDefaultYsonCodec() {
        assertSame(DefaultYsonCodec.INSTANCE, CodecRegistry.DEFAULT.getYsonCodec());
    }

    @Test
    public void getInstanceIsIdempotent() {
        var first = CodecRegistry.getInstance();
        var second = CodecRegistry.getInstance();
        assertSame(first, second);
    }
}

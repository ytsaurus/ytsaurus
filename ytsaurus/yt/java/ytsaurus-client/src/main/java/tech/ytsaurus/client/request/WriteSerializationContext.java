package tech.ytsaurus.client.request;

import tech.ytsaurus.client.rows.WireRowSerializer;

public class WriteSerializationContext<T> extends SerializationContext<T> {

    public WriteSerializationContext(WireRowSerializer<T> serializer) {
        this.wireSerializer = serializer;
    }

}

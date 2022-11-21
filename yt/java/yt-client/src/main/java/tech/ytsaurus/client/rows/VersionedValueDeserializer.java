package tech.ytsaurus.client.rows;

public class VersionedValueDeserializer extends AbstractValueDeserializer<VersionedValue> {
    @Override
    public VersionedValue build() {
        return new VersionedValue(id, type, aggregate, value, timestamp);
    }
}

package tech.ytsaurus.client.rows;

public class UnversionedValueDeserializer extends AbstractValueDeserializer<UnversionedValue> {
    @Override
    public UnversionedValue build() {
        return new UnversionedValue(id, type, aggregate, value);
    }
}

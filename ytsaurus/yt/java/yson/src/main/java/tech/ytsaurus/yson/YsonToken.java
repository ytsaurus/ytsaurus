package tech.ytsaurus.yson;

import java.util.List;

import javax.annotation.Nullable;

/**
 * Represent yson token.
 */
public class YsonToken {
    @Nullable
    private final Object value;
    private final YsonTokenType type;

    private YsonToken(@Nullable Object value, YsonTokenType type) {
        this.value = value;
        this.type = type;
    }

    /**
     * Creates string token.
     */
    public static YsonToken stringValue(String value) {
        return new YsonToken(value, YsonTokenType.String);
    }

    /**
     * Creates int64 token.
     */
    public static YsonToken int64Value(Long value) {
        return new YsonToken(value, YsonTokenType.Int64);
    }

    /**
     * Creates uint64 token.
     */
    public static YsonToken uint64Value(Long value) {
        return new YsonToken(value, YsonTokenType.Uint64);
    }

    /**
     * Creates double token.
     */
    public static YsonToken doubleValue(Double value) {
        return new YsonToken(value, YsonTokenType.Double);
    }

    /**
     * Creates boolean token.
     */
    public static YsonToken booleanValue(Boolean value) {
        return new YsonToken(value, YsonTokenType.Boolean);
    }

    /**
     * Creates hash token.
     */
    public static YsonToken entityValue() {
        return new YsonToken(null, YsonTokenType.Hash);
    }

    /**
     * Creates end_of_stream token.
     */
    public static YsonToken endOfStream() {
        return new YsonToken(null, YsonTokenType.EndOfStream);
    }

    /**
     * Creates start_of_stream token.
     */
    public static YsonToken startOfStream() {
        return new YsonToken(null, YsonTokenType.StartOfStream);
    }

    /**
     * Creates token from character (like bracket, comma or others).
     */
    public static YsonToken fromSymbol(byte ch) {
        return new YsonToken((char) ch, YsonTokenType.fromSymbol(ch));
    }

    /**
     * Converts string token to String.
     * Fails if it is not string.
     */
    public String asStringValue() {
        expectType(YsonTokenType.String);
        return (String) value;
    }

    /**
     * Converts int64 token to Long.
     * Fails if it is not int64.
     */
    public Long asInt64Value() {
        expectType(YsonTokenType.Int64);
        return (Long) value;
    }

    /**
     * Converts uint64 token to Long.
     * Fails if it is not uint64.
     */
    public Long asUint64Value() {
        expectType(YsonTokenType.Uint64);
        return (Long) value;
    }

    /**
     * Converts double token to Double.
     * Fails if it is not double.
     */
    public Double asDoubleValue() {
        expectType(YsonTokenType.Double);
        return (Double) value;
    }

    /**
     * Converts boolean token to Boolean.
     * Fails if it is not boolean.
     */
    public Boolean asBooleanValue() {
        expectType(YsonTokenType.Boolean);
        return (Boolean) value;
    }

    /**
     * @return type of token
     */
    public YsonTokenType getType() {
        return type;
    }

    /**
     * Check type of token.
     */
    public void expectType(YsonTokenType expectedType) {
        if (type != expectedType) {
            throw new IllegalStateException("Unexpected token type: " + type + ", expected: " + expectedType);
        }
    }

    /**
     * Check type of token.
     */
    public void expectTypes(List<YsonTokenType> expectedTypes) {
        if (!expectedTypes.contains(type)) {
            throw new IllegalStateException("Expected one of: " + expectedTypes + " types, but actual type is " + type);
        }
    }

    @Override
    public String toString() {
        return "YsonToken[ " + type + " | " + value + " ]";
    }
}

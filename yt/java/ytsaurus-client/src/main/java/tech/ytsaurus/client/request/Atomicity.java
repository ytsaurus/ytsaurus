package tech.ytsaurus.client.request;

import tech.ytsaurus.rpcproxy.EAtomicity;

/**
 * Atomicity of transaction or table.
 *
 * @see <a href="https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/sorted-dynamic-tables">
 * dynamic table documentation
 * </a>
 */
public enum Atomicity {
    Full(EAtomicity.A_FULL, "full"),
    None(EAtomicity.A_NONE, "none");

    private final EAtomicity protoValue;
    private final String stringValue;

    Atomicity(EAtomicity protoValue, String stringValue) {
        this.protoValue = protoValue;
        this.stringValue = stringValue;
    }

    @Override
    public String toString() {
        return stringValue;
    }

    public EAtomicity getProtoValue() {
        return protoValue;
    }
}

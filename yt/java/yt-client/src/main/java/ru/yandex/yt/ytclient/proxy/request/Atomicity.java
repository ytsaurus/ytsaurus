package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.yt.rpcproxy.EAtomicity;

/**
 * Atomicity of transaction or table.
 *
 * @see <a href="https://docs.yandex-team.ru/yt/description/dynamic_tables/sorted_dynamic_tables#atomarnost">
 *     dynamic table documentation
 *     </a>
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

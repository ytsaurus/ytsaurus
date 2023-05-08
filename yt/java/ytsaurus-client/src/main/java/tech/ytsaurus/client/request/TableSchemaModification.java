package tech.ytsaurus.client.request;

import tech.ytsaurus.rpcproxy.ETableSchemaModification;

/**
 * @see
 * <a href="https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/bulk-insert#deletions-and-extended-write-format">
 * Deletions and extended write format
 * </a>
 */
public enum TableSchemaModification {
    NONE(ETableSchemaModification.TSM_NONE, "none"),
    UNVERSIONED_UPDATE(ETableSchemaModification.TSM_UNVERSIONED_UPDATE, "unversioned_update"),
    UNVERSIONED_UPDATE_UNSORTED(ETableSchemaModification.TSM_UNVERSIONED_UPDATE_UNSORTED,
            "unversioned_update_unsorted");

    private final ETableSchemaModification protoValue;
    private final String stringValue;

    TableSchemaModification(ETableSchemaModification protoValue, String stringValue) {
        this.protoValue = protoValue;
        this.stringValue = stringValue;
    }

    @Override
    public String toString() {
        return stringValue;
    }

    public ETableSchemaModification getProtoValue() {
        return protoValue;
    }
}

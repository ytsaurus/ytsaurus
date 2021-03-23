package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.cypress.CypressNodeType;

public enum ObjectType {
    // Static nodes
    StringNode(300, CypressNodeType.STRING),
    Int64Node(301, CypressNodeType.INT64),
    Uint64Node(306, CypressNodeType.UINT64),
    DoubleNode(302, CypressNodeType.DOUBLE),
    MapNode(303, CypressNodeType.MAP),
    ListNode(304, CypressNodeType.LIST),
    BooleanNode(305, CypressNodeType.BOOLEAN),

    // Dynamic nodes
    File(400, CypressNodeType.FILE),
    Table(401, CypressNodeType.TABLE),
    Link(417, null),
    Document(421, CypressNodeType.DOCUMENT),
    ReplicatedTable(425, CypressNodeType.REPLICATED_TABLE),

    // Tablet Manager stuff
    TableReplica(709, CypressNodeType.TABLE_REPLICA);

    private final int value;
    private final CypressNodeType cypressNodeType;

    ObjectType(int value, CypressNodeType cypressNodeType) {
        this.value = value;
        this.cypressNodeType = cypressNodeType;
    }

    public static ObjectType from(CypressNodeType type) {
        switch (type) {
            case STRING:
                return StringNode;
            case INT64:
                return Int64Node;
            case UINT64:
                return Uint64Node;
            case DOUBLE:
                return DoubleNode;
            case BOOLEAN:
                return BooleanNode;
            case MAP:
                return MapNode;
            case LIST:
                return ListNode;
            case FILE:
                return File;
            case TABLE:
                return Table;
            case DOCUMENT:
                return Document;
            case REPLICATED_TABLE:
                return ReplicatedTable;
            case TABLE_REPLICA:
                return TableReplica;
            default:
                throw new IllegalArgumentException("Unsupported type " + type);
        }
    }

    public int value() {
        return value;
    }

    public CypressNodeType toCypressNodeType() {
        if (cypressNodeType == null) {
            throw new IllegalStateException("Cypress node type not exists in " + this);
        }
        return cypressNodeType;
    }
}

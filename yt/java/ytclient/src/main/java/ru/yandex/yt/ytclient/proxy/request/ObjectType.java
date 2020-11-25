package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.cypress.CypressNodeType;

public enum ObjectType {
    Null (0),


    // Static nodes
    StringNode (300),
    Int64Node (301),
    Uint64Node (306),
    DoubleNode (302),
    MapNode (303),
    ListNode (304),
    BooleanNode (305),

    // Dynamic nodes
    File (400),
    Table (401),
    Journal (423),
    Orchid (412),
    Link (417),
    Document (421),
    ReplicatedTable (425),

    // Tablet Manager stuff
    TableReplica (709),
    ;

    private final int value;

    ObjectType(int value) {
        this.value = value;
    }

    static ObjectType from(CypressNodeType type) {
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
                throw new IllegalArgumentException();
        }
    }

    public int value() {
        return value;
    }
}

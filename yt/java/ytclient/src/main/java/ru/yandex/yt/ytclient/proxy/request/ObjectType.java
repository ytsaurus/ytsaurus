package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.cypress.CypressNodeType;

public enum ObjectType {
    Null (0),

    // The following represent non-versioned objects.
    // These must be created by calling TMasterYPathProxy::CreateObjects.

    // Transaction Manager stuff
    Transaction (  1),
    AtomicTabletTransaction (  2),
    NonAtomicTabletTransaction (  3),
    NestedTransaction (  4),
    TransactionMap (407),
    TopmostTransactionMap (418),
    LockMap (422),

    // Chunk Manager stuff
    Chunk (100),
    ErasureChunk (102), // erasure chunk as a whole
    ErasureChunkPart_0 (103), // erasure chunk parts, mnemonic names are for debugging convenience only
    ErasureChunkPart_1 (104),
    ErasureChunkPart_2 (105),
    ErasureChunkPart_3 (106),
    ErasureChunkPart_4 (107),
    ErasureChunkPart_5 (108),
    ErasureChunkPart_6 (109),
    ErasureChunkPart_7 (110),
    ErasureChunkPart_8 (111),
    ErasureChunkPart_9 (112),
    ErasureChunkPart_10 (113),
    ErasureChunkPart_11 (114),
    ErasureChunkPart_12 (115),
    ErasureChunkPart_13 (116),
    ErasureChunkPart_14 (117),
    ErasureChunkPart_15 (118),
    JournalChunk (119),
    Artifact (121),
    ChunkMap (402),
    LostChunkMap (403),
    LostVitalChunkMap (413),
    PrecariousChunkMap (410),
    PrecariousVitalChunkMap (411),
    OverreplicatedChunkMap (404),
    UnderreplicatedChunkMap (405),
    DataMissingChunkMap (419),
    ParityMissingChunkMap (420),
    QuorumMissingChunkMap (424),
    UnsafelyPlacedChunkMap (120),
    ForeignChunkMap (122),
    ChunkList (101),
    ChunkListMap (406),
    Medium (408),
    MediumMap (409),

    // The following represent versioned objects (AKA Cypress nodes).
    // These must be created by calling TCypressYPathProxy::Create.
    // NB: When adding a new type, don't forget to update IsVersionedType.

    // Auxiliary
    Lock (200),

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

    // Security Manager stuff
    Account (500),
    AccountMap (414),
    User (501),
    UserMap (415),
    Group (502),
    GroupMap (416),

    // Global stuff
    // A mysterious creature representing the master as a whole.
    Master (600),
    ClusterCell (601),
    SysNode (602),

    // Tablet Manager stuff
    TabletCell (700),
    TabletCellNode (701),
    Tablet (702),
    TabletMap (703),
    TabletCellMap (710),
    SortedDynamicTabletStore (704),
    OrderedDynamicTabletStore (708),
    TabletPartition (705),
    TabletCellBundle (706),
    TabletCellBundleMap (707),
    TableReplica (709),
    TabletAction (711),
    TabletActionMap (712),

    // Node Tracker stuff
    Rack (800),
    RackMap (801),
    ClusterNode (802),
    ClusterNodeNode (803),
    ClusterNodeMap (804),
    DataCenter (805),
    DataCenterMap (806),


    // Job Tracker stuff
    SchedulerJob (900),
    MasterJob (901),

    // Scheduler
    Operation (1000);

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

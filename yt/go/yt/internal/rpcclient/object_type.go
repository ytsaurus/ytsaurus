package rpcclient

import (
	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/yt"
)

type ObjectType int32

const (
	ObjectTypeNull                          ObjectType = 0
	ObjectTypeTransaction                   ObjectType = 1
	ObjectTypeAtomicTabletTransaction       ObjectType = 2
	ObjectTypeNonAtomicTabletTransaction    ObjectType = 3
	ObjectTypeNestedTransaction             ObjectType = 4
	ObjectTypeExternalizedTransaction       ObjectType = 5
	ObjectTypeExternalizedNestedTransaction ObjectType = 6
	ObjectTypeUploadTransaction             ObjectType = 7
	ObjectTypeUploadNestedTransaction       ObjectType = 8
	ObjectTypeTransactionMap                ObjectType = 407
	ObjectTypeTopmostTransactionMap         ObjectType = 418
	ObjectTypeLockMap                       ObjectType = 422

	// Chunk Manager stuff
	ObjectTypeChunk                     ObjectType = 100
	ObjectTypeErasureChunk              ObjectType = 102 // erasure chunk as a whole
	ObjectTypeErasureChunkPart0         ObjectType = 103 // erasure chunk parts, mnemonic names are for debugging convenience only
	ObjectTypeErasureChunkPart1         ObjectType = 104
	ObjectTypeErasureChunkPart2         ObjectType = 105
	ObjectTypeErasureChunkPart3         ObjectType = 106
	ObjectTypeErasureChunkPart4         ObjectType = 107
	ObjectTypeErasureChunkPart5         ObjectType = 108
	ObjectTypeErasureChunkPart6         ObjectType = 109
	ObjectTypeErasureChunkPart7         ObjectType = 110
	ObjectTypeErasureChunkPart8         ObjectType = 111
	ObjectTypeErasureChunkPart9         ObjectType = 112
	ObjectTypeErasureChunkPart10        ObjectType = 113
	ObjectTypeErasureChunkPart11        ObjectType = 114
	ObjectTypeErasureChunkPart12        ObjectType = 115
	ObjectTypeErasureChunkPart13        ObjectType = 116
	ObjectTypeErasureChunkPart14        ObjectType = 117
	ObjectTypeErasureChunkPart15        ObjectType = 118
	ObjectTypeJournalChunk              ObjectType = 119
	ObjectTypeArtifact                  ObjectType = 121
	ObjectTypeChunkMap                  ObjectType = 402
	ObjectTypeLostChunkMap              ObjectType = 403
	ObjectTypeLostVitalChunkMap         ObjectType = 413
	ObjectTypePrecariousChunkMap        ObjectType = 410
	ObjectTypePrecariousVitalChunkMap   ObjectType = 411
	ObjectTypeOverreplicatedChunkMap    ObjectType = 404
	ObjectTypeUnderreplicatedChunkMap   ObjectType = 405
	ObjectTypeDataMissingChunkMap       ObjectType = 419
	ObjectTypeParityMissingChunkMap     ObjectType = 420
	ObjectTypeOldestPartMissingChunkMap ObjectType = 428
	ObjectTypeQuorumMissingChunkMap     ObjectType = 424
	ObjectTypeUnsafelyPlacedChunkMap    ObjectType = 120
	ObjectTypeForeignChunkMap           ObjectType = 122
	ObjectTypeChunkList                 ObjectType = 101
	ObjectTypeChunkListMap              ObjectType = 406
	ObjectTypeChunkView                 ObjectType = 123
	ObjectTypeChunkViewMap              ObjectType = 430
	ObjectTypeDomesticMedium            ObjectType = 408
	ObjectTypeMediumMap                 ObjectType = 409
	ObjectTypeErasureJournalChunk       ObjectType = 124 // erasure journal chunk as a whole
	ObjectTypeErasureJournalChunkPart0  ObjectType = 125 // erasure chunk parts, mnemonic names are for debugging convenience only
	ObjectTypeErasureJournalChunkPart1  ObjectType = 126
	ObjectTypeErasureJournalChunkPart2  ObjectType = 127
	ObjectTypeErasureJournalChunkPart3  ObjectType = 128
	ObjectTypeErasureJournalChunkPart4  ObjectType = 129
	ObjectTypeErasureJournalChunkPart5  ObjectType = 130
	ObjectTypeErasureJournalChunkPart6  ObjectType = 131
	ObjectTypeErasureJournalChunkPart7  ObjectType = 132
	ObjectTypeErasureJournalChunkPart8  ObjectType = 133
	ObjectTypeErasureJournalChunkPart9  ObjectType = 134
	ObjectTypeErasureJournalChunkPart10 ObjectType = 135
	ObjectTypeErasureJournalChunkPart11 ObjectType = 136
	ObjectTypeErasureJournalChunkPart12 ObjectType = 137
	ObjectTypeErasureJournalChunkPart13 ObjectType = 138
	ObjectTypeErasureJournalChunkPart14 ObjectType = 139
	ObjectTypeErasureJournalChunkPart15 ObjectType = 140

	// The following represent versioned objects (AKA Cypress nodes).
	// These must be created by calling TCypressYPathProxy::Create.
	// NB: When adding a new type, don't forget to update IsVersionedType.

	// Auxiliary
	ObjectTypeLock ObjectType = 200

	// Static nodes
	ObjectTypeStringNode  ObjectType = 300
	ObjectTypeInt64Node   ObjectType = 301
	ObjectTypeUint64Node  ObjectType = 306
	ObjectTypeDoubleNode  ObjectType = 302
	ObjectTypeMapNode     ObjectType = 303
	ObjectTypeListNode    ObjectType = 304
	ObjectTypeBooleanNode ObjectType = 305

	// Dynamic nodes
	ObjectTypeFile            ObjectType = 400
	ObjectTypeTable           ObjectType = 401
	ObjectTypeJournal         ObjectType = 423
	ObjectTypeOrchid          ObjectType = 412
	ObjectTypeLink            ObjectType = 417
	ObjectTypeDocument        ObjectType = 421
	ObjectTypeReplicatedTable ObjectType = 425

	// Portals
	ObjectTypePortalEntrance    ObjectType = 11000
	ObjectTypePortalExit        ObjectType = 11001
	ObjectTypePortalEntranceMap ObjectType = 11002
	ObjectTypePortalExitMap     ObjectType = 11003
	ObjectTypeCypressShard      ObjectType = 11004
	ObjectTypeCypressShardMap   ObjectType = 11005

	// Security Manager stuff
	ObjectTypeAccount           ObjectType = 500
	ObjectTypeAccountMap        ObjectType = 414
	ObjectTypeUser              ObjectType = 501
	ObjectTypeUserMap           ObjectType = 415
	ObjectTypeGroup             ObjectType = 502
	ObjectTypeGroupMap          ObjectType = 416
	ObjectTypeNetworkProject    ObjectType = 503
	ObjectTypeNetworkProjectMap ObjectType = 426
	ObjectTypeProxyRole         ObjectType = 504
	ObjectTypeHTTPProxyRoleMap  ObjectType = 427
	ObjectTypeRPCProxyRoleMap   ObjectType = 429

	// Global stuff
	// A mysterious creature representing the master as a whole.
	ObjectTypeMaster     ObjectType = 600
	ObjectTypeMasterCell ObjectType = 601
	ObjectTypeSysNode    ObjectType = 602

	// Tablet Manager stuff
	ObjectTypeTabletCell                ObjectType = 700
	ObjectTypeTabletCellNode            ObjectType = 701
	ObjectTypeTablet                    ObjectType = 702
	ObjectTypeTabletMap                 ObjectType = 703
	ObjectTypeTabletCellMap             ObjectType = 710
	ObjectTypeSortedDynamicTabletStore  ObjectType = 704
	ObjectTypeOrderedDynamicTabletStore ObjectType = 708
	ObjectTypeTabletPartition           ObjectType = 705
	ObjectTypeTabletCellBundle          ObjectType = 706
	ObjectTypeTabletCellBundleMap       ObjectType = 707
	ObjectTypeTableReplica              ObjectType = 709
	ObjectTypeTabletAction              ObjectType = 711
	ObjectTypeTabletActionMap           ObjectType = 712

	// Node Tracker stuff
	ObjectTypeRack            ObjectType = 800
	ObjectTypeRackMap         ObjectType = 801
	ObjectTypeClusterNode     ObjectType = 802
	ObjectTypeClusterNodeNode ObjectType = 803
	ObjectTypeClusterNodeMap  ObjectType = 804
	ObjectTypeDataCenter      ObjectType = 805
	ObjectTypeDataCenterMap   ObjectType = 806

	// Job Tracker stuff
	ObjectTypeSchedulerJob ObjectType = 900
	ObjectTypeMasterJob    ObjectType = 901

	// Scheduler
	ObjectTypeOperation            ObjectType = 1000
	ObjectTypeSchedulerPool        ObjectType = 1001
	ObjectTypeSchedulerPoolTree    ObjectType = 1002
	ObjectTypeSchedulerPoolTreeMap ObjectType = 1003

	// Object manager stuff
	ObjectTypeEstimatedCreationTimeMap ObjectType = 1100
)

func convertObjectType(typ yt.NodeType) (ObjectType, error) {
	var ret ObjectType

	switch typ {
	case yt.NodeMap:
		ret = ObjectTypeMapNode
	case yt.NodeLink:
		ret = ObjectTypeLink
	case yt.NodeFile:
		ret = ObjectTypeFile
	case yt.NodeTable:
		ret = ObjectTypeTable
	case yt.NodeString:
		ret = ObjectTypeStringNode
	case yt.NodeBoolean:
		ret = ObjectTypeBooleanNode
	case yt.NodeDocument:
		ret = ObjectTypeDocument
	case yt.NodeTableReplica:
		ret = ObjectTypeTableReplica
	case yt.NodeReplicatedTable:
		ret = ObjectTypeReplicatedTable
	case yt.NodeUser:
		ret = ObjectTypeUser
	case yt.NodeGroup:
		ret = ObjectTypeGroup
	case yt.NodeAccount:
		ret = ObjectTypeAccount
	case yt.NodeDomesticMedium:
		ret = ObjectTypeDomesticMedium
	case yt.NodeTabletCellBundle:
		ret = ObjectTypeTabletCellBundle
	case yt.NodeSys:
		ret = ObjectTypeSysNode
	case yt.NodePortalEntrance:
		ret = ObjectTypePortalEntrance
	case yt.NodePortalExit:
		ret = ObjectTypePortalExit
	case yt.NodeSchedulerPool:
		ret = ObjectTypeSchedulerPool
	case yt.NodeSchedulerPoolTree:
		ret = ObjectTypeSchedulerPoolTree
	default:
		return 0, xerrors.Errorf("unsupported node type %q", typ)
	}

	return ret, nil
}

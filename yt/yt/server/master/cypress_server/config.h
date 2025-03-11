#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/library/erasure/public.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

struct TDynamicCypressManagerConfig
    : public NYTree::TYsonStruct
{
    int DefaultFileReplicationFactor;

    int DefaultTableReplicationFactor;

    NErasure::ECodec DefaultJournalErasureCodec;
    int DefaultJournalReplicationFactor;
    int DefaultJournalReadQuorum;
    int DefaultJournalWriteQuorum;

    NErasure::ECodec DefaultHunkStorageErasureCodec;
    int DefaultHunkStorageReplicationFactor;
    int DefaultHunkStorageReadQuorum;
    int DefaultHunkStorageWriteQuorum;

    //! Period between Cypress access statistics commits.
    TDuration StatisticsFlushPeriod;

    //! Maximum number of children map and list nodes are allowed to contain.
    int MaxNodeChildCount;

    //! Maximum allowed length of string nodes.
    int MaxStringNodeLength;

    //! Maximum allowed size of custom attributes for objects (transactions, Cypress nodes etc).
    //! This limit concerns the binary YSON representation of attributes.
    int MaxAttributeSize;

    //! Maximum allowed length of keys in map nodes.
    int MaxMapNodeKeyLength;

    TDuration ExpirationCheckPeriod;
    int MaxExpiredNodesRemovalsPerCommit;
    TDuration ExpirationBackoffTime;
    bool RemoveExpiredMasterNodesViaClient;

    NCompression::ECodec TreeSerializationCodec;

    TDuration RecursiveResourceUsageCacheExpirationTimeout;

    double DefaultExternalCellBias;

    TDuration PortalSynchronizationPeriod;

    // COMPAT(kvk1920)
    bool EnablePortalSynchronization;

    bool EnableSymlinkCyclicityCheck;

    // COMPAT(shakurov)
    bool AllowCrossShardDynamicTableCopying;

    TDuration ScionRemovalPeriod;

    // COMPAT(kvk1920)
    bool ForbidListNodeCreation;

    int MaxLocksPerTransactionSubtree;

    std::optional<i64> VirtualMapReadOffloadBatchSize;

    int CrossCellCopyMaxSubtreeSize;

    // COMPAT(cherepashka)
    bool EnableInheritAttributesDuringCopy;

    // COMPAT(danilalexeev)
    bool DisableCypressNodeReachability;

    // COMPAT(shakurov)
    bool EnableIntraCellCrossShardLinks;

    // COMPAT(koloshmet)
    bool EnableCrossCellLinks;

    // COMPAT(koloshmet)
    bool EnablePreserveAclDuringMove;

    i64 MaxAttributeFilterSizeToLog;

    REGISTER_YSON_STRUCT(TDynamicCypressManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicCypressManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

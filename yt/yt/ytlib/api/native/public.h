#pragma once

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/api/operation_client.h>

#include <yt/yt/client/tablet_client/public.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IConnection)
DECLARE_REFCOUNTED_STRUCT(IClient)
DECLARE_REFCOUNTED_STRUCT(ITransaction)
DECLARE_REFCOUNTED_CLASS(TClientCache)
DECLARE_REFCOUNTED_CLASS(TStickyGroupSizeCache)
DECLARE_REFCOUNTED_CLASS(TSyncReplicaCache)
DECLARE_REFCOUNTED_CLASS(TTabletSyncReplicaCache)
DECLARE_REFCOUNTED_STRUCT(IStickyMountCache)
DECLARE_REFCOUNTED_STRUCT(ITableReplicaSynchronicityCache)

DECLARE_REFCOUNTED_STRUCT(ICellCommitSession)
DECLARE_REFCOUNTED_STRUCT(ICellCommitSessionProvider)
DECLARE_REFCOUNTED_STRUCT(ITabletCommitSession)
DECLARE_REFCOUNTED_STRUCT(ITabletRequestBatcher)

DECLARE_REFCOUNTED_STRUCT(TMasterConnectionConfig)
DECLARE_REFCOUNTED_STRUCT(TMasterCacheConnectionConfig)
DECLARE_REFCOUNTED_STRUCT(TCypressProxyConnectionConfig)
DECLARE_REFCOUNTED_STRUCT(TClockServersConfig)
DECLARE_REFCOUNTED_STRUCT(TSequoiaConnectionConfig)

DECLARE_REFCOUNTED_STRUCT(TConnectionStaticConfig)
DECLARE_REFCOUNTED_STRUCT(TConnectionDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TConnectionCompoundConfig)

using TConnectionDynamicConfigAtomicPtr = TAtomicIntrusivePtr<TConnectionDynamicConfig>;

DECLARE_REFCOUNTED_CLASS(TJournalChunkWriterOptions)

struct TConnectionOptions;

class TTabletSyncReplicaCache;
class TTabletReplicaStatusCache;

using TTableReplicaInfoPtrList = TCompactVector<
    NTabletClient::TTableReplicaInfoPtr,
    NTabletClient::TypicalTableReplicaCount>;

using TTableReplicaIdList = TCompactVector<
    NTabletClient::TTableReplicaId,
    NTabletClient::TypicalTableReplicaCount>;

//! See comment in helpers.h
DEFINE_ENUM(EClusterConnectionDynamicConfigPolicy,
    (FromStaticConfig)
    (FromClusterDirectoryWithStaticPatch)
    (FromClusterDirectory)
);

DEFINE_BIT_ENUM_WITH_UNDERLYING_TYPE(EChaosResidencyCacheType, i8,
    (MasterCache)
    (Client)
);

// TODO(omgronny): Do we really two separate vectors for finished and running jobs?
struct TListJobsFromControllerAgentResult
{
    std::vector<TJob> FinishedJobs;
    int TotalFinishedJobCount = 0;
    std::vector<TJob> InProgressJobs;
    int TotalInProgressJobCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative


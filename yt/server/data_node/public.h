#pragma once

#include <yt/server/chunk_server/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/misc/enum.h>
#include <yt/core/misc/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

using NChunkClient::TChunkId;
using NChunkClient::TSessionId;
using NChunkClient::ESessionType;

using NChunkServer::TBlockId;

using NNodeTrackerClient::TNodeId;

////////////////////////////////////////////////////////////////////////////////

struct TChunkDescriptor;
struct TSessionOptions;
struct TBlockReadOptions;

class TPendingIOGuard;
class TChunkReadGuard;

struct TArtifactKey;

DECLARE_REFCOUNTED_CLASS(TMasterConnector)

DECLARE_REFCOUNTED_CLASS(TChunkStore)
DECLARE_REFCOUNTED_CLASS(TChunkCache)
DECLARE_REFCOUNTED_CLASS(TChunkRegistry)

DECLARE_REFCOUNTED_CLASS(TChunkBlockManager)
DECLARE_REFCOUNTED_CLASS(TBlobReaderCache)
DECLARE_REFCOUNTED_CLASS(TJournalDispatcher)

DECLARE_REFCOUNTED_CLASS(TCachedChunkMeta)
DECLARE_REFCOUNTED_CLASS(TChunkMetaManager)

DECLARE_REFCOUNTED_CLASS(TLocation)
DECLARE_REFCOUNTED_CLASS(TStoreLocation)
DECLARE_REFCOUNTED_CLASS(TCacheLocation)
DECLARE_REFCOUNTED_CLASS(TJournalManager)

DECLARE_REFCOUNTED_STRUCT(IChunk)
DECLARE_REFCOUNTED_CLASS(TCachedBlock)
DECLARE_REFCOUNTED_CLASS(TStoredBlobChunk)
DECLARE_REFCOUNTED_CLASS(TCachedBlobChunk)
DECLARE_REFCOUNTED_CLASS(TJournalChunk)

DECLARE_REFCOUNTED_STRUCT(ISession)
DECLARE_REFCOUNTED_CLASS(TBlobSession)
DECLARE_REFCOUNTED_CLASS(TSessionManager)

DECLARE_REFCOUNTED_CLASS(TPeerBlockDistributor)
DECLARE_REFCOUNTED_CLASS(TPeerBlockTable)
DECLARE_REFCOUNTED_CLASS(TPeerBlockUpdater)

DECLARE_REFCOUNTED_CLASS(TPeerBlockTableConfig)
DECLARE_REFCOUNTED_CLASS(TStoreLocationConfigBase)
DECLARE_REFCOUNTED_CLASS(TStoreLocationConfig)
DECLARE_REFCOUNTED_CLASS(TCacheLocationConfig)
DECLARE_REFCOUNTED_CLASS(TMultiplexedChangelogConfig)
DECLARE_REFCOUNTED_CLASS(TArtifactCacheReaderConfig)
DECLARE_REFCOUNTED_CLASS(TRepairReaderConfig)
DECLARE_REFCOUNTED_CLASS(TSealReaderConfig)
DECLARE_REFCOUNTED_CLASS(TDataNodeConfig)
DECLARE_REFCOUNTED_CLASS(TPeerBlockDistributorConfig)
DECLARE_REFCOUNTED_CLASS(TPeerBlockTableConfig)
DECLARE_REFCOUNTED_CLASS(TLayerLocationConfig)
DECLARE_REFCOUNTED_CLASS(TVolumeManagerConfig)

DECLARE_REFCOUNTED_STRUCT(IVolume)
DECLARE_REFCOUNTED_STRUCT(IVolumeManager)
DECLARE_REFCOUNTED_STRUCT(IPlainVolumeManager)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EErrorCode,
    ((LocalChunkReaderFailed)    (1300))
);

DEFINE_ENUM(EDirectIOPolicy,
    (Always)
    (Never)
    (ForSyncOnCloseChunks)
);

DEFINE_ENUM(EIOEngineType,
    (ThreadPool)
    (Aio)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

#pragma once

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/misc/public.h>

#include <yt/core/misc/public.h>

namespace NYT {
namespace NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TNodeStatistics;
class TNodeResources;
class TNodeResourceLimitsOverrides;

class TDiskResources;

class TAddressMap;
class TNodeAddressMap;

class TNodeDescriptor;
class TNodeDirectory;

class TReqRegisterNode;
class TRspRegisterNode;

class TReqIncrementalHeartbeat;
class TRspIncrementalHeartbeat;

class TReqFullHeartbeat;
class TRspFullHeartbeat;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EErrorCode,
    ((NoSuchNode)        (1600))
    ((InvalidState)      (1601))
    ((NoSuchNetwork)     (1602))
    ((NoSuchRack)        (1603))
    ((NoSuchDataCenter)  (1604))
);

DEFINE_ENUM(EAddressType,
    ((InternalRpc)    (0))
    ((SkynetHttp)     (1))
);

using TNodeId = ui32;
const TNodeId InvalidNodeId = 0;
const TNodeId MaxNodeId = (1 << 24) - 1; // TNodeId must fit into 24 bits (see TChunkReplica)

using TRackId = NObjectClient::TObjectId;

using TDataCenterId = NObjectClient::TObjectId;

// Only domain names, without port number.
using TNetworkAddressList = std::vector<std::pair<TString, TString>>;
using TNetworkPreferenceList = std::vector<TString>;

// Network -> host:port.
using TAddressMap = THashMap<TString, TString>;

// Address Type (e.g. rpc, http) -> Network -> host:port.
using TNodeAddressMap = THashMap<EAddressType, TAddressMap>;

class TNodeDescriptor;
class TNodeDirectoryBuilder;

DECLARE_REFCOUNTED_CLASS(TNodeDirectory)
DECLARE_REFCOUNTED_CLASS(TNodeDirectorySynchronizer)

DECLARE_REFCOUNTED_CLASS(TNodeDirectorySynchronizerConfig)

DECLARE_REFCOUNTED_STRUCT(INodeChannelFactory)

extern const TString DefaultNetworkName;
extern const TNetworkPreferenceList DefaultNetworkPreferences;

DEFINE_ENUM(EMemoryCategory,
    ((Footprint)                   (0))
    ((BlockCache)                  (1))
    ((ChunkMeta)                   (2))
    ((UserJobs)                    (3))
    ((TabletStatic)                (4))
    ((TabletDynamic)               (5))
    ((BlobSession)                 (6))
    ((CachedVersionedChunkMeta)    (7))
    ((SystemJobs)                  (8))
);

using TNodeMemoryTracker = TMemoryUsageTracker<EMemoryCategory>;
using TNodeMemoryTrackerGuard = TMemoryUsageTrackerGuard<EMemoryCategory>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerClient
} // namespace NYT

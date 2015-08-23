#pragma once

#include <core/misc/public.h>

#include <ytlib/object_client/public.h>

namespace NYT {
namespace NNodeTrackerClient {

///////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TNodeStatistics;
class TNodeResources;

class TNodeDescriptor;
class TNodeDirectory;

class TReqRegisterNode;
class TRspRegisterNode;

class TReqIncrementalHeartbeat;
class TRspIncrementalHeartbeat;

class TReqFullHeartbeat;
class TRspFullHeartbeat;

} // namespace NProto

///////////////////////////////////////////////////////////////////////////////

using TNodeId = i32;
const TNodeId InvalidNodeId = 0;
const TNodeId MaxNodeId = (1 << 28) - 1; // TNodeId must fit into 28 bits (see TChunkReplica)

using TRackId = NObjectClient::TObjectId;
extern const TRackId NullRackId;

using TAddressMap = yhash_map<Stroka, Stroka>;
class TNodeDescriptor;

class TNodeDirectoryBuilder;

DECLARE_REFCOUNTED_CLASS(TNodeDirectory)

extern const Stroka DefaultNetworkName;
extern const Stroka InterconnectNetworkName;

DEFINE_ENUM(EErrorCode,
    ((NoSuchNode)    (1600))
    ((InvalidState)  (1601))
    ((NoSuchNetwork) (1602))
    ((NoSuchRack)    (1603))
);

DEFINE_ENUM(EMemoryCategory,
    ((Footprint)      (0))
    ((BlockCache)     (1))
    ((ChunkMeta)      (2))
    ((Jobs)           (3))
    ((TabletStatic)   (4))
    ((TabletDynamic)  (5))
    ((BlobSession)    (6))
);

///////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerClient
} // namespace NYT

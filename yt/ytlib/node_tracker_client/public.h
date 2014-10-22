#pragma once

#include <core/misc/common.h>

namespace NYT {
namespace NNodeTrackerClient {

///////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TNodeDirectory;

class TReqRegisterNode;
class TRspRegisterNode;

class TReqIncrementalHeartbeat;
class TRspIncrementalHeartbeat;

class TReqFullHeartbeat;
class TRspFullHeartbeat;

} // namespace NProto

///////////////////////////////////////////////////////////////////////////////

typedef i32 TNodeId;
const TNodeId InvalidNodeId = 0;
const TNodeId MaxNodeId = (1 << 28) - 1; // TNodeId must fit into 28 bits (see TChunkReplica)

class TNodeDescriptor;

DECLARE_REFCOUNTED_CLASS(TNodeDirectory)

extern const Stroka DefaultNetworkName;

class TNodeDirectoryBuilder;

DECLARE_ENUM(EErrorCode,
    ((NoSuchNode)    (300))
    ((InvalidState)  (301))
    ((NoSuchNetwork) (302))
);

///////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerClient
} // namespace NYT

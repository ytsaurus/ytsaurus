#pragma once

#include "public.h"

#include <yt/server/chunk_server/chunk_replica.h>

#include <yt/client/node_tracker_client/proto/node.pb.h>

namespace NYT {
namespace NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

//! A helper for building node directories in fetch handlers.
class TNodeDirectoryBuilder
    : private TNonCopyable
{
public:
    explicit TNodeDirectoryBuilder(
        NNodeTrackerClient::NProto::TNodeDirectory* protoDirectory,
        NNodeTrackerClient::EAddressType addressType = NNodeTrackerClient::EAddressType::InternalRpc);

    void Add(const TNode* node);
    void Add(NChunkServer::TNodePtrWithIndexes node);
    void Add(const NChunkServer::TNodePtrWithIndexesList& nodes);

private:
    NNodeTrackerClient::NProto::TNodeDirectory* ProtoDirectory_;
    const NNodeTrackerClient::EAddressType AddressType_;

    THashSet<TNodeId> ListedNodeIds_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT

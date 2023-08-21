#pragma once

#include "public.h"

#include <yt/yt/server/master/chunk_server/chunk_replica.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

namespace NYT::NNodeTrackerServer {

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
    void Add(NChunkServer::TNodePtrWithReplicaAndMediumIndex node);
    void Add(TRange<NChunkServer::TNodePtrWithReplicaAndMediumIndex> nodeList);
    void Add(NChunkServer::TChunkLocationPtrWithReplicaInfo location);
    void Add(TRange<NChunkServer::TChunkLocationPtrWithReplicaInfo> locationList);

private:
    NNodeTrackerClient::NProto::TNodeDirectory* ProtoDirectory_;
    const NNodeTrackerClient::EAddressType AddressType_;

    THashSet<TNodeId> ListedNodeIds_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer

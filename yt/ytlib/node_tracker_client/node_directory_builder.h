#pragma once

#include "public.h"

#include <yt/client/chunk_client/chunk_replica.h>

#include <yt/ytlib/node_tracker_client/public.h>

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

//! A helper for building node directories in RPC responses.
class TNodeDirectoryBuilder
{
public:
    TNodeDirectoryBuilder(
        TNodeDirectoryPtr directory,
        NNodeTrackerClient::NProto::TNodeDirectory* protoDirectory);

    void Add(NChunkClient::TChunkReplica replica);
    void Add(const NChunkClient::TChunkReplicaList& replicas);

private:
    const TNodeDirectoryPtr Directory_;
    NProto::TNodeDirectory* const ProtoDirectory_;

    THashSet<TNodeId> ListedNodeIds_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient

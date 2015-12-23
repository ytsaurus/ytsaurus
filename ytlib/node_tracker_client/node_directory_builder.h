#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/chunk_replica.h>

#include <yt/ytlib/node_tracker_client/node.pb.h>

namespace NYT {
namespace NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

//! A helper for building node directories in operation controllers.
class TNodeDirectoryBuilder
{
public:
    explicit TNodeDirectoryBuilder(
        TNodeDirectoryPtr directory,
        NNodeTrackerClient::NProto::TNodeDirectory* protoDirectory);

    void Add(NChunkClient::TChunkReplica replica);

private:
    TNodeDirectoryPtr Directory;
    NProto::TNodeDirectory* ProtoDirectory;
    yhash_set<TNodeId> ListedNodeIds;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerClient
} // namespace NYT

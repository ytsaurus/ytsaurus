#pragma once

#include "public.h"

#include <yt/server/chunk_server/chunk_replica.h>

#include <yt/ytlib/node_tracker_client/node.pb.h>

namespace NYT {
namespace NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

//! A helper for building node directories in fetch handlers.
class TNodeDirectoryBuilder
    : private TNonCopyable
{
public:
    explicit TNodeDirectoryBuilder(NNodeTrackerClient::NProto::TNodeDirectory* protoDirectory);

    void Add(const TNode* node);
    void Add(NChunkServer::TNodePtrWithIndexes node);
    void Add(const NChunkServer::TNodePtrWithIndexesList& nodes);

private:
    NNodeTrackerClient::NProto::TNodeDirectory* ProtoDirectory;
    yhash_set<TNodeId> ListedNodeIds;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT

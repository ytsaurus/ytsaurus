#pragma once

#include "public.h"

#include <ytlib/node_tracker_client/node.pb.h>

#include <server/chunk_server/chunk_replica.h>

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
    void Add(NChunkServer::TNodePtrWithIndex node);
    void Add(const NChunkServer::TNodePtrWithIndexList& nodes);

private:
    NNodeTrackerClient::NProto::TNodeDirectory* ProtoDirectory;
    yhash_set<TNodeId> ListedNodeIds;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT

#pragma once

#include "public.h"
#include "chunk_replica.h"

#include <ytlib/node_tracker_client/node.pb.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! A helper for building node directories in fetch handlers.
class TNodeDirectoryBuilder
{
public:
    explicit TNodeDirectoryBuilder(NNodeTrackerClient::NProto::TNodeDirectory* protoDirectory);

    void Add(TNode* node);
    void Add(TNodePtrWithIndex node);
    void Add(const TNodePtrWithIndexList& nodes);

private:
    NNodeTrackerClient::NProto::TNodeDirectory* ProtoDirectory;
    yhash_set<TNodeId> ListedNodeIds;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

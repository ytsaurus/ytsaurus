#pragma once

#include "public.h"
#include "chunk_replica.h"

#include <ytlib/chunk_client/node.pb.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! A helper for building node directories in fetch handlers.
class TNodeDirectoryBuilder
{
public:
    explicit TNodeDirectoryBuilder(NChunkClient::NProto::TNodeDirectory* protoDirectory);

    void Add(TDataNode* node);
    void Add(TDataNodeWithIndex node);
    void Add(const TDataNodeWithIndexList& nodes);

private:
    NChunkClient::NProto::TNodeDirectory* ProtoDirectory;
    yhash_set<TNodeId> ListedNodeIds;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

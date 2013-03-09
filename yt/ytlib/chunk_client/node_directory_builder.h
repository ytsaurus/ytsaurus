#pragma once

#include "public.h"
#include "chunk_replica.h"

#include <ytlib/chunk_client/node.pb.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! A helper for building node directories in fetch handlers.
class TNodeDirectoryBuilder
{
public:
    explicit TNodeDirectoryBuilder(
        TNodeDirectoryPtr directory,
        NChunkClient::NProto::TNodeDirectory* protoDirectory);

    void Add(TChunkReplica replica);

private:
    TNodeDirectoryPtr Directory;
    NProto::TNodeDirectory* ProtoDirectory;
    yhash_set<TNodeId> ListedNodeIds;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

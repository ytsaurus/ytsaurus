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
    void Add(TChunkReplica replica);
    void Add(const TChunkReplicaList& replicas);

private:
    NChunkClient::NProto::TNodeDirectory* ProtoDirectory;
    yhash_set<TNodeId> ListedNodeIds;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

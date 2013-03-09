#include "stdafx.h"
#include "node_directory_builder.h"
#include "node_directory.h"

#include <ytlib/misc/foreach.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

TNodeDirectoryBuilder::TNodeDirectoryBuilder(
    TNodeDirectoryPtr directory,
    NChunkClient::NProto::TNodeDirectory* protoDirectory)
    : Directory(directory)
    , ProtoDirectory(protoDirectory)
{ }

void TNodeDirectoryBuilder::Add(TChunkReplica replica)
{
    auto nodeId = replica.GetNodeId();
    if (!ListedNodeIds.insert(nodeId).second)
        return;
    const auto& descriptor = Directory->GetDescriptor(replica);
    auto* item = ProtoDirectory->add_items();
    item->set_node_id(nodeId);
    ToProto(item->mutable_node_descriptor(), descriptor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT


#include "node_directory_builder.h"
#include "node_directory.h"

#include <yt/core/misc/common.h>

namespace NYT {
namespace NNodeTrackerClient {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TNodeDirectoryBuilder::TNodeDirectoryBuilder(
    TNodeDirectoryPtr directory,
    NProto::TNodeDirectory* protoDirectory)
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

} // namespace NNodeTrackerClient
} // namespace NYT


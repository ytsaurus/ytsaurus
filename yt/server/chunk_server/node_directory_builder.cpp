#include "stdafx.h"
#include "node_directory_builder.h"

#include <server/node_tracker_server/node.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

TNodeDirectoryBuilder::TNodeDirectoryBuilder(NNodeTrackerClient::NProto::TNodeDirectory* protoDirectory)
    : ProtoDirectory(protoDirectory)
{ }

void TNodeDirectoryBuilder::Add(TNode* node)
{
    if (!ListedNodeIds.insert(node->GetId()).second)
        return;

    auto* item = ProtoDirectory->add_items();
    item->set_node_id(node->GetId());
    ToProto(item->mutable_node_descriptor(), node->GetDescriptor());
}

void TNodeDirectoryBuilder::Add(TNodePtrWithIndex node)
{
    Add(node.GetPtr());
}

void TNodeDirectoryBuilder::Add(const TNodePtrWithIndexList& nodes)
{
    FOREACH (auto node, nodes) {
        Add(node);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT


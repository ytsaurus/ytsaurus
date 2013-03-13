#include "stdafx.h"
#include "node_directory_builder.h"
#include "node.h"

#include <ytlib/misc/foreach.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

TNodeDirectoryBuilder::TNodeDirectoryBuilder(NChunkClient::NProto::TNodeDirectory* protoDirectory)
    : ProtoDirectory(protoDirectory)
{ }

void TNodeDirectoryBuilder::Add(TDataNode* node)
{
    if (!ListedNodeIds.insert(node->GetId()).second)
        return;

    auto* item = ProtoDirectory->add_items();
    item->set_node_id(node->GetId());
    ToProto(item->mutable_node_descriptor(), node->GetDescriptor());
}

void TNodeDirectoryBuilder::Add(TDataNodePtrWithIndex node)
{
    Add(node.GetPtr());
}

void TNodeDirectoryBuilder::Add(const TDataNodePtrWithIndexList& nodes)
{
    FOREACH (auto node, nodes) {
        Add(node);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT


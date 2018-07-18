#include "node_directory_builder.h"

#include <yt/server/node_tracker_server/node.h>

#include <yt/client/node_tracker_client/proto/node_directory.pb.h>

namespace NYT {
namespace NNodeTrackerServer {

using namespace NNodeTrackerClient;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

TNodeDirectoryBuilder::TNodeDirectoryBuilder(NNodeTrackerClient::NProto::TNodeDirectory* protoDirectory, EAddressType addressType)
    : ProtoDirectory_(protoDirectory)
    , AddressType_(addressType)
{ }

void TNodeDirectoryBuilder::Add(const TNode* node)
{
    if (!ListedNodeIds_.insert(node->GetId()).second)
        return;

    auto* item = ProtoDirectory_->add_items();
    item->set_node_id(node->GetId());
    ToProto(item->mutable_node_descriptor(), node->GetDescriptor(AddressType_));
}

void TNodeDirectoryBuilder::Add(TNodePtrWithIndexes node)
{
    Add(node.GetPtr());
}

void TNodeDirectoryBuilder::Add(const TNodePtrWithIndexesList& nodes)
{
    for (auto node : nodes) {
        Add(node);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT


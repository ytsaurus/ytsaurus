#include "node_directory_builder.h"

#include "node.h"

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node_directory.pb.h>

namespace NYT::NNodeTrackerServer {

using namespace NNodeTrackerClient;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

TNodeDirectoryBuilder::TNodeDirectoryBuilder(NNodeTrackerClient::NProto::TNodeDirectory* protoDirectory, EAddressType addressType)
    : ProtoDirectory_(protoDirectory)
    , AddressType_(addressType)
{ }

void TNodeDirectoryBuilder::Add(const TNode* node)
{
    if (!ListedNodeIds_.insert(node->GetId()).second) {
        return;
    }

    auto* item = ProtoDirectory_->add_items();
    item->set_node_id(ToProto<ui32>(node->GetId()));
    ToProto(item->mutable_node_descriptor(), node->GetDescriptor(AddressType_));
}

void TNodeDirectoryBuilder::Add(TNodePtrWithReplicaAndMediumIndex node)
{
    Add(node.GetPtr());
}

void TNodeDirectoryBuilder::Add(TChunkLocationPtrWithReplicaInfo location)
{
    Add(GetChunkLocationNode(location));
}

void TNodeDirectoryBuilder::Add(TRange<TNodePtrWithReplicaAndMediumIndex> nodes)
{
    for (auto node : nodes) {
        Add(node);
    }
}

void TNodeDirectoryBuilder::Add(TRange<TChunkLocationPtrWithReplicaInfo> locationList)
{
    for (auto location : locationList) {
        Add(location);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer


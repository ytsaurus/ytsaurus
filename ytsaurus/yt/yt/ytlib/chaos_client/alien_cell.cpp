#include "alien_cell.h"

#include <yt/yt/core/misc/protobuf_helpers.h> 

namespace NYT::NChaosClient {

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TAlienCellDescriptorLite* protoDescriptor, const TAlienCellDescriptorLite& descriptor)
{
    ToProto(protoDescriptor->mutable_cell_id(), descriptor.CellId);
    protoDescriptor->set_config_version(descriptor.ConfigVersion);
}

void FromProto(TAlienCellDescriptorLite* descriptor, const NProto::TAlienCellDescriptorLite& protoDescriptor)
{
    FromProto(&descriptor->CellId, protoDescriptor.cell_id());
    descriptor->ConfigVersion = protoDescriptor.config_version();
}

void ToProto(NProto::TAlienCellDescriptor* protoDescriptor, const TAlienCellDescriptor& descriptor)
{
    ToProto(protoDescriptor->mutable_cell_id(), descriptor.CellId);
    protoDescriptor->set_config_version(descriptor.ConfigVersion);
    ToProto(protoDescriptor->mutable_alien_peers(), descriptor.AlienPeers);
}

void FromProto(TAlienCellDescriptor* descriptor, const NProto::TAlienCellDescriptor& protoDescriptor)
{
    FromProto(&descriptor->CellId, protoDescriptor.cell_id());
    descriptor->ConfigVersion = protoDescriptor.config_version();
    FromProto(&descriptor->AlienPeers, protoDescriptor.alien_peers());
}

void ToProto(NProto::TAlienPeerDescriptor* protoDescriptor, const TAlienPeerDescriptor& descriptor)
{
    protoDescriptor->set_peer_id(descriptor.PeerId);
    ToProto(protoDescriptor->mutable_node_descriptor(), descriptor.NodeDescriptor);
}

void FromProto(TAlienPeerDescriptor* descriptor, const NProto::TAlienPeerDescriptor& protoDescriptor)
{
    descriptor->PeerId = protoDescriptor.peer_id();
    FromProto(&descriptor->NodeDescriptor, protoDescriptor.node_descriptor());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient

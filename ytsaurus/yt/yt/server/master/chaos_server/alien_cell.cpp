#include "alien_cell.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NChaosServer {

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(NProto::TAlienPeerDescriptor* protoDescriptor, const NChaosServer::TAlienPeerDescriptor& descriptor)
{
    NChaosServer::ToProto(protoDescriptor, descriptor);
}

void FromProto(NChaosServer::TAlienPeerDescriptor* descriptor, const NProto::TAlienPeerDescriptor& protoDescriptor)
{
    NChaosServer::FromProto(descriptor, protoDescriptor);
}

void ToProto(NProto::TAlienCellDescriptor* protoDescriptor, const NChaosServer::TAlienCellDescriptor& descriptor)
{
    NChaosServer::ToProto(protoDescriptor, descriptor);
}

void FromProto(NChaosServer::TAlienCellDescriptor* descriptor, const NProto::TAlienCellDescriptor& protoDescriptor)
{
    NChaosServer::FromProto(descriptor, protoDescriptor);
}

} // namespace NProto

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

void ToProto(NProto::TAlienCellConstellation* protoConstellation, const TAlienCellConstellation& constellation)
{
    protoConstellation->set_alien_cluster_index(constellation.AlienClusterIndex);
    ToProto(protoConstellation->mutable_alien_cells(), constellation.AlienCells);
    ToProto(protoConstellation->mutable_lost_alien_cell_ids(), constellation.LostAlienCellIds);
    protoConstellation->set_enable_metadata_cells(constellation.EnableMetadataCells);
}

void FromProto(TAlienCellConstellation* constellation, const NProto::TAlienCellConstellation& protoConstellation)
{
    constellation->AlienClusterIndex = protoConstellation.alien_cluster_index();
    FromProto(&constellation->AlienCells, protoConstellation.alien_cells());
    FromProto(&constellation->LostAlienCellIds, protoConstellation.lost_alien_cell_ids());
    constellation->EnableMetadataCells = protoConstellation.enable_metadata_cells();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer

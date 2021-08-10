#pragma once

#include <yt/yt/ytlib/chaos_client/proto/chaos_master_service.pb.h>

#include <yt/yt/client/election/public.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

struct TAlienCellDescriptorLite
{
    NObjectClient::TCellId CellId;
    int ConfigVersion;
};

struct TAlienPeerDescriptor
{
    NElection::TPeerId PeerId;
    NNodeTrackerClient::TNodeDescriptor NodeDescriptor;
};

struct TAlienCellDescriptor
    : public TAlienCellDescriptorLite
{
    std::vector<TAlienPeerDescriptor> AlienPeers;
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NChaosClient::NProto::TAlienCellDescriptorLite* protoDescriptor, const TAlienCellDescriptorLite& descriptor);
void FromProto(TAlienCellDescriptorLite* descriptor, const NChaosClient::NProto::TAlienCellDescriptorLite& protoDescriptor);

void ToProto(NChaosClient::NProto::TAlienCellDescriptor* protoDescriptor, const TAlienCellDescriptor& descriptor);
void FromProto(TAlienCellDescriptor* descriptor, const NChaosClient::NProto::TAlienCellDescriptor& protoDescriptor);

void ToProto(NChaosClient::NProto::TAlienPeerDescriptor* protoDescriptor, const TAlienPeerDescriptor& descriptor);
void FromProto(TAlienPeerDescriptor* descriptor, const NChaosClient::NProto::TAlienPeerDescriptor& protoDescriptor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient

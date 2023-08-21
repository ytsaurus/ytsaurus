#pragma once

#include "public.h"

#include <yt/yt/server/master/chaos_server/proto/alien_cell.pb.h>

#include <yt/yt/ytlib/chaos_client/alien_cell.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

using TAlienCellDescriptorLite = NChaosClient::TAlienCellDescriptorLite;
using TAlienCellDescriptor = NChaosClient::TAlienCellDescriptor;
using TAlienPeerDescriptor = NChaosClient::TAlienPeerDescriptor;

struct TAlienCellConstellation
{
    int AlienClusterIndex;
    std::vector<TAlienCellDescriptor> AlienCells;
    std::vector<NObjectClient::TCellId> LostAlienCellIds;
    bool EnableMetadataCells;
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TAlienCellDescriptorLite* protoDescriptor, const TAlienCellDescriptorLite& descriptor);
void FromProto(TAlienCellDescriptorLite* descriptor, const NProto::TAlienCellDescriptorLite& protoDescriptor);

void ToProto(NProto::TAlienCellDescriptor* protoDescriptor, const TAlienCellDescriptor& descriptor);
void FromProto(TAlienCellDescriptor* descriptor, const NProto::TAlienCellDescriptor& protoDescriptor);

void ToProto(NProto::TAlienPeerDescriptor* protoDescriptor, const TAlienPeerDescriptor& descriptor);
void FromProto(TAlienPeerDescriptor* descriptor, const NProto::TAlienPeerDescriptor& protoDescriptor);

void ToProto(NProto::TAlienCellConstellation* protoConstellation, const TAlienCellConstellation& constellation);
void FromProto(TAlienCellConstellation* constellation, const NProto::TAlienCellConstellation& protoConstellation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer

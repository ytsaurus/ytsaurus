#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt_proto/yt/client/cell_master/proto/cell_directory.pb.h>

namespace NYT::NCellMasterClient {

using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TServiceDiscoveryEndpointsConfig* protoServiceDiscoveryEndpointsConfig,
    const NRpc::TServiceDiscoveryEndpointsConfigPtr& serviceDiscoveryEndpointsConfig);

void FromProto(
    NRpc::TServiceDiscoveryEndpointsConfigPtr* serviceDiscoveryEndpointsConfig,
    const NProto::TServiceDiscoveryEndpointsConfig& protoServiceDiscoveryEndpointsConfig);

void ToProto(
    NProto::TCellDirectoryItem* protoCellDirectoryItem,
    const NApi::NNative::TMasterConnectionConfigPtr& masterConnectionConfig);

void FromProto(
    NApi::NNative::TMasterConnectionConfigPtr* masterConnectionConfig,
    const NProto::TCellDirectoryItem& protoCellDirectoryItem);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMasterClient

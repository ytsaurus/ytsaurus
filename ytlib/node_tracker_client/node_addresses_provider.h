#pragma once

#include "public.h"

#include <yt/client/api/client.h>

#include <yt/core/rpc/public.h>

#include <yt/core/actions/public.h>

#include <yt/ytlib/cell_master_client/public.h>

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

NRpc::IChannelPtr CreateNodeAddressesChannel(
    TDuration syncPeriod,
    TWeakPtr<NCellMasterClient::TCellDirectory> cellDirectory,
    ENodeRole nodeRole,
    TCallback<NRpc::IChannelPtr(const std::vector<TString>&)> channelBuilder);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient

#pragma once

#include "public.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/ytlib/cell_master_client/public.h>

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

NRpc::IChannelPtr CreateNodeAddressesChannel(
    TDuration syncPeriod,
    TDuration syncPeriodSplay,
    TWeakPtr<NCellMasterClient::TCellDirectory> cellDirectory,
    ENodeRole nodeRole,
    TCallback<NRpc::IChannelPtr(const std::vector<TString>&)> channelBuilder);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient

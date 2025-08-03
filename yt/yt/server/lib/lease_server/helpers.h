#pragma once

#include "public.h"

#include <yt/yt/server/lib/hive/public.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NLeaseServer {

////////////////////////////////////////////////////////////////////////////////

TFuture<void> IssueLeasesForCell(
    const std::vector<NObjectClient::TTransactionId>& prerequisiteTransactionIds,
    const NLeaseServer::ILeaseManagerPtr& leaseManager,
    const NHiveServer::IHiveManagerPtr& hiveManager,
    NObjectClient::TCellId selfCellId,
    bool syncWithAllLeaseTransactionCoordinators,
    std::function<NObjectClient::TCellId(NObjectClient::TCellTag)> getMasterCellId,
    std::function<NRpc::IChannelPtr(NObjectClient::TCellTag)> findMasterChannel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLeaseServer

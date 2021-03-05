#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/core/rpc/config.h>

namespace NYT::NOrchid {

////////////////////////////////////////////////////////////////////////////////

struct TOrchidManifest
    : public NRpc::TRetryingChannelConfig
{
    NYTree::INodePtr RemoteAddresses;
    TString RemoteRoot;
    TDuration Timeout;

    TOrchidManifest()
    {
        RegisterParameter("remote_addresses", RemoteAddresses);
        RegisterParameter("remote_root", RemoteRoot)
            .Default("/");
        RegisterParameter("timeout", Timeout)
            .Default(TDuration::Seconds(60));

        RegisterPostprocessor([&] {
            RetryAttempts = 1;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TOrchidManifest)

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateOrchidTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrchid

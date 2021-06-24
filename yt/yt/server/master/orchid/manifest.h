#pragma once

#include "public.h"

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

} // namespace NYT::NOrchid

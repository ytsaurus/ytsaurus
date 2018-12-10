#pragma once

#include "public.h"

#include <yt/core/bus/tcp/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

class TTraceManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Address where all trace events are pushed to.
    //! If null then push is disabled.
    std::optional<TString> Address;

    //! Timeout for push requests.
    TDuration RpcTimeout;

    //! Maximum number of trace events per batch.
    int MaxBatchSize;

    //! Send period.
    TDuration SendPeriod;

    //! Port to show in endpoints.
    ui16 EndpointPort;

    // Bus config.
    NBus::TTcpBusConfigPtr BusClient;

    TTraceManagerConfig()
    {
        RegisterParameter("address", Address)
            .Default();
        RegisterParameter("rpc_timeout", RpcTimeout)
            .Default();
        RegisterParameter("max_batch_size", MaxBatchSize)
            .Default(100);
        RegisterParameter("send_period", SendPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("endpoint_port", EndpointPort)
            .Default(0);
        RegisterParameter("bus_client", BusClient)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TTraceManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing


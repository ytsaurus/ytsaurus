#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NTracing {

////////////////////////////////////////////////////////////////////////////////

class TTraceManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Address where all trace events are pushed to.
    //! If |Null| then push is disabled.
    TNullable<Stroka> Address;

    //! Maximum number of trace events per batch.
    int MaxBatchSize;

    //! Send period.
    TDuration SendPeriod;

    //! Port to show in endpoints.
    ui16 EndpointPort;

    TTraceManagerConfig()
    {
        RegisterParameter("address", Address)
            .Default(Null);
        RegisterParameter("max_batch_size", MaxBatchSize)
            .Default(100);
        RegisterParameter("send_period", SendPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("endpoint_port", EndpointPort)
            .Default(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TTraceManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTracing
} // namespace NYT


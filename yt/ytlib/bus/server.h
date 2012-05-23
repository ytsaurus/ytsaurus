#pragma once

#include "common.h"
#include "bus.h"

#include <ytlib/ytree/public.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

struct TBusStatistics
{
    TBusStatistics()
        : RequestCount(0)
        , RequestDataSize(0)
        , ResponseCount(0)
        , ResponseDataSize(0)
    { }

    i64 RequestCount;
    i64 RequestDataSize;
    i64 ResponseCount;
    i64 ResponseDataSize;
};

struct IBusServer
    : public virtual TRefCounted
{
    typedef TIntrusivePtr<IBusServer> TPtr;

    virtual void Start(IMessageHandler::TPtr handler) = 0;
    virtual void Stop() = 0;

    virtual void GetMonitoringInfo(NYTree::IYsonConsumer* consumer) = 0;
    virtual TBusStatistics GetStatistics() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT

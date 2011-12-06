#pragma once

#include "common.h"
#include "bus.h"

#include "../misc/config.h"
#include "../ytree/ytree_fwd.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

struct IBusServer
    : virtual TRefCountedBase
{
    typedef TIntrusivePtr<IBusServer> TPtr;

    virtual void Start(IMessageHandler* handler) = 0;
    virtual void Stop() = 0;
    virtual void GetMonitoringInfo(NYTree::IYsonConsumer* consumer) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT

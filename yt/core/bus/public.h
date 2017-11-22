#pragma once

#include <yt/core/misc/public.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IBus)
DECLARE_REFCOUNTED_STRUCT(IMessageHandler)
DECLARE_REFCOUNTED_STRUCT(IBusClient)
DECLARE_REFCOUNTED_STRUCT(IBusServer)

struct TTcpDispatcherStatistics;

DECLARE_REFCOUNTED_CLASS(TTcpBusConfig)
DECLARE_REFCOUNTED_CLASS(TTcpBusServerConfig)
DECLARE_REFCOUNTED_CLASS(TTcpBusClientConfig)

extern const TString DefaultNetworkName;
extern const TString LocalNetworkName;

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT


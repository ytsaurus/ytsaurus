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

DECLARE_REFCOUNTED_STRUCT(TTcpDispatcherCounters)

using TTosLevel = int;
constexpr int DefaultTosLevel = 0;

constexpr size_t MaxMessagePartCount = 1 << 28;
constexpr size_t MaxMessagePartSize = 1_GB;

DEFINE_ENUM(EDeliveryTrackingLevel,
    (None)
    (ErrorOnly)
    (Full)
);

DEFINE_ENUM(EErrorCode,
    ((TransportError)               (100))
);

extern const TString DefaultNetworkName;
extern const TString LocalNetworkName;

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT


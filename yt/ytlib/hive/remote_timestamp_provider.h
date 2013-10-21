#pragma once

#include "public.h"

#include <core/rpc/public.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

ITimestampProviderPtr CreateRemoteTimestampProvider(
    TRemoteTimestampProviderConfigPtr config,
    NRpc::IChannelPtr channel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT

#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

struct IClockManager
    : public TRefCounted
{
    virtual const ITimestampProviderPtr& GetTimestampProviderOrThrow(NObjectClient::TCellTag clockClusterTag) = 0;
    virtual void ValidateDefaultClock(TStringBuf message) = 0;
    virtual NObjectClient::TCellTag GetCurrentClockTag() = 0;
    virtual void Reconfigure(TClockManagerConfigPtr newConfig) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClockManager)

////////////////////////////////////////////////////////////////////////////////

IClockManagerPtr CreateClockManager(
    TClockManagerConfigPtr config,
    NApi::NNative::IConnectionPtr connection,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient

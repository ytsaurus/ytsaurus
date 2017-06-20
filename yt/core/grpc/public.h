#pragma once

#include <yt/core/logging/log.h>

namespace NYT {
namespace NGrpc {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TServerAddressConfig)
DECLARE_REFCOUNTED_CLASS(TServerConfig)
DECLARE_REFCOUNTED_CLASS(TChannelConfig)

////////////////////////////////////////////////////////////////////////////////

extern const char* const RequestIdMetadataKey;
extern const char* const ErrorMetadataKey;

constexpr int GenericErrorStatusCode = 100;

////////////////////////////////////////////////////////////////////////////////

} // namespace NGrpc
} // namespace NYT

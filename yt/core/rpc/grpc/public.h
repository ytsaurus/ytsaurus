#pragma once

#include <yt/core/logging/log.h>

namespace NYT::NRpc::NGrpc {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSslPemKeyCertPairConfig)
DECLARE_REFCOUNTED_CLASS(TServerCredentialsConfig)
DECLARE_REFCOUNTED_CLASS(TServerAddressConfig)
DECLARE_REFCOUNTED_CLASS(TServerConfig)
DECLARE_REFCOUNTED_CLASS(TChannelCredentialsConfig)
DECLARE_REFCOUNTED_CLASS(TChannelConfig)

////////////////////////////////////////////////////////////////////////////////

extern const char* const RequestIdMetadataKey;
extern const char* const UserMetadataKey;
extern const char* const UserAgentMetadataKey;
extern const char* const AuthTokenMetadataKey;
extern const char* const AuthSessionIdMetadataKey;
extern const char* const AuthSslSessionIdMetadataKey;
extern const char* const AuthUserTicketMetadataKey;
extern const char* const ErrorMetadataKey;
extern const char* const MessageBodySizeMetadataKey;
extern const char* const ProtocolVersionMetadataKey;

constexpr int GenericErrorStatusCode = 100;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc

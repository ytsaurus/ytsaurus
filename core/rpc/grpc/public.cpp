#include "public.h"

namespace NYT {
namespace NRpc {
namespace NGrpc {

////////////////////////////////////////////////////////////////////////////////

const char* const RequestIdMetadataKey = "yt-request-id";
const char* const UserMetadataKey = "yt-user";
const char* const UserAgentMetadataKey = "user-agent";
const char* const AuthTokenMetadataKey = "yt-auth-token";
const char* const AuthSessionIdMetadataKey = "yt-auth-session-id";
const char* const AuthSslSessionIdMetadataKey = "yt-auth-ssl-session-id";
const char* const ErrorMetadataKey = "yt-error-bin";
const char* const MessageBodySizeMetadataKey = "yt-message-body-size";
const char* const ProtocolVersionMetadataKey = "yt-protocol-version";

////////////////////////////////////////////////////////////////////////////////

} // namespace NGrpc
} // namespace NRpc
} // namespace NYT

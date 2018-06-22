#include "public.h"

namespace NYT {
namespace NRpc {
namespace NGrpc {

////////////////////////////////////////////////////////////////////////////////

const char* const RequestIdMetadataKey = "yt-request-id";
const char* const UserMetadataKey = "yt-user";
const char* const TokenMetadataKey = "yt-token";
const char* const ErrorMetadataKey = "yt-error-bin";
const char* const MessageBodySizeMetadataKey = "yt-message-body-size";

////////////////////////////////////////////////////////////////////////////////

} // namespace NGrpc
} // namespace NRpc
} // namespace NYT

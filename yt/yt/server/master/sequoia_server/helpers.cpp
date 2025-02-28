#include "helpers.h"

namespace NYT::NSequoiaServer {

using namespace NSequoiaClient;

////////////////////////////////////////////////////////////////////////////////

TError CheckSequoiaReign(ESequoiaReign requestReign)
{
    auto currentReign = GetCurrentSequoiaReign();
    if (requestReign != currentReign) {
        return TError(NSequoiaClient::EErrorCode::InvalidSequoiaReign, "Invalid request Sequoia reign")
            << TErrorAttribute("request_reign", ToString(requestReign))
            << TErrorAttribute("current_reign", ToString(currentReign));
    }

    return TError{};
}

void ValidateSequoiaReign(ESequoiaReign requestReign)
{
    THROW_ERROR_EXCEPTION_IF_FAILED(CheckSequoiaReign(requestReign));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer

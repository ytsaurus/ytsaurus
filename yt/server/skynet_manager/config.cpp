#include "config.h"

namespace NYT {
namespace NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

void TClusterConnectionConfig::LoadToken()
{
    if (OAuthTokenEnv.empty()) {
        return;
    }

    auto token = getenv(OAuthTokenEnv.c_str());
    if (!token) {
        THROW_ERROR_EXCEPTION("%v environment variable is not set", OAuthTokenEnv);
    }
    OAuthToken = token;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT

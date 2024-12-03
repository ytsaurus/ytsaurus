#include "crypto.h"

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <contrib/libs/libsodium/include/sodium.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void InitializeCryptography()
{
    // NB(pavook) sodium_init might stall if there's not enough entropy in the system
    // (see https://docs.libsodium.org/usage). We need to set a reasonable timeout on this operation.
    auto initFuture = BIND(sodium_init)
        .AsyncVia(GetCurrentInvoker())
        .Run()
        .WithTimeout(CryptoInitializeTimeout);

    auto initResult = WaitFor(initFuture);

    if (initResult.FindMatching(NYT::EErrorCode::Timeout)) {
        THROW_ERROR_EXCEPTION(
            "timeout exceeded on libsodium initialization, ensure there is enough entropy in the system");
    }

    if (!initResult.IsOK() || initResult.Value() < 0) {
        THROW_ERROR_EXCEPTION("libsodium initialization failed");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

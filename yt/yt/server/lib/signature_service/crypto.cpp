#include "crypto.h"

#include <yt/yt/core/misc/error.h>

#include <contrib/libs/libsodium/include/sodium.h>

namespace NYT::NSignatureService {

////////////////////////////////////////////////////////////////////////////////

void InitializeCryptography()
{
    // TODO(pavook) sodium_init might stall if there's not enough entropy in the system
    // (see https://docs.libsodium.org/usage). We need to set a reasonable timeout on this operation
    // and give a verbose error message on failure.
    if (sodium_init() < 0) {
        THROW_ERROR_EXCEPTION("libsodium initialization failed");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignatureService

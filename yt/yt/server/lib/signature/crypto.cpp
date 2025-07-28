#include "crypto.h"

#include "private.h"

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <contrib/libs/libsodium/include/sodium.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

std::atomic<bool> CryptographyInitializationFinished = {false};

} // namespace

TFuture<void> InitializeCryptography(const IInvokerPtr& invoker)
{
    // NB(pavook) sodium_init might stall if there's not enough entropy in the system
    // (see https://docs.libsodium.org/usage). We need to set a reasonable timeout on this operation.
    return BIND(sodium_init)
        .AsyncVia(invoker)
        .Run()
        .Apply(BIND([] (int initResult) {
            if (initResult < 0) {
                YT_LOG_ALERT("libsodium initialization failed.");
            } else {
                CryptographyInitializationFinished.store(true, std::memory_order::release);
            }
        }));
}

void EnsureCryptographyInitialized()
{
    if (!CryptographyInitializationFinished.load(std::memory_order::acquire)) {
        THROW_ERROR_EXCEPTION(NRpc::EErrorCode::TransientFailure, "Cryptography subsystem hasn't been initialized yet");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

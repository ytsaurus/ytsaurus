#include "key_rotator.h"

#include "config.h"
#include "private.h"
#include "signature_generator.h"

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TKeyRotator::TKeyRotator(
    TKeyRotatorConfigPtr config,
    IInvokerPtr invoker,
    TSignatureGeneratorPtr generator)
    : Config_(std::move(config))
    , Executor_(New<TPeriodicExecutor>(
        std::move(invoker),
        BIND([generator = std::move(generator)] () {
            auto error = WaitFor(generator->Rotate());
            if (!error.IsOK()) {
                YT_LOG_ERROR(error, "Failed to rotate keypair");
            }
        }),
        Config_->KeyRotationInterval))
{
    YT_LOG_INFO("Key rotator initialized (KeyRotationInterval %v)", Config_->KeyRotationInterval);
}

void TKeyRotator::Start()
{
    YT_LOG_DEBUG("Starting key rotation");
    Executor_->Start();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

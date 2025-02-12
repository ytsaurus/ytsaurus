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
        BIND([this, generator = std::move(generator)] () {
            auto error = WaitFor(generator->Rotate());
            if (!error.IsOK()) {
                YT_LOG_ERROR(error, "Failed to rotate keypair");
            }
            // NB(pavook): this spinlock is needed to avoid skipping first rotation in Start().
            auto guard = Guard(SpinLock_);
        }),
        Config_->KeyRotationInterval))
{
    YT_LOG_INFO("Key rotator initialized (KeyRotationInterval %v)", Config_->KeyRotationInterval);
}

TFuture<void> TKeyRotator::Start()
{
    YT_LOG_DEBUG("Starting key rotation");
    auto guard = Guard(SpinLock_);
    Executor_->Start();
    return Executor_->GetExecutedEvent();
}

TFuture<void> TKeyRotator::Stop()
{
    YT_LOG_DEBUG("Stopping key rotation");
    auto guard = Guard(SpinLock_);
    return Executor_->Stop();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

#include "key_rotator.h"

#include "config.h"
#include "key_store.h"
#include "private.h"
#include "signature_generator.h"

#include <yt/yt/core/concurrency/retrying_periodic_executor.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TKeyRotator::TKeyRotator(
    TKeyRotatorConfigPtr config,
    IInvokerPtr invoker,
    IKeyStoreWriterPtr keyWriter,
    TSignatureGeneratorPtr generator)
    : Config_(std::move(config))
    , KeyWriter_(std::move(keyWriter))
    , Generator_(std::move(generator))
    , Executor_(New<TRetryingPeriodicExecutor>(
        std::move(invoker),
        BIND_NO_PROPAGATE([weakSelf = MakeWeak(this)] {
            if (auto self = weakSelf.Lock()) {
                return self->DoRotate();
            }
            return TError();
        }),
        Config_.Acquire()->KeyRotationOptions))
{
    YT_LOG_INFO("Key rotator initialized (KeyRotationInterval: %v)", Config_.Acquire()->KeyRotationOptions.Period);
}


TFuture<void> TKeyRotator::Start()
{
    YT_LOG_DEBUG("Starting key rotation");
    return Executor_->StartAndGetFirstExecutedEvent();
}

TFuture<void> TKeyRotator::Stop()
{
    YT_LOG_DEBUG("Stopping key rotation");
    return Executor_->Stop();
}

TFuture<void> TKeyRotator::Rotate()
{
    auto event = Executor_->GetExecutedEvent();
    Executor_->ScheduleOutOfBand();
    return event;
}

void TKeyRotator::Reconfigure(TKeyRotatorConfigPtr config)
{
    YT_VERIFY(config);
    auto keyRotationOptions = config->KeyRotationOptions;
    {
        auto guard = Guard(ReconfigureSpinLock_);
        Config_.Store(std::move(config));
        Executor_->SetOptions(keyRotationOptions);
    }
    YT_LOG_INFO("Key rotator reconfigured (KeyRotationInterval: %v, Splay: %v, Jitter: %v)",
        keyRotationOptions.Period,
        keyRotationOptions.Splay,
        keyRotationOptions.Jitter);
}

////////////////////////////////////////////////////////////////////////////////

TError TKeyRotator::DoRotate()
{
    auto currentKeyInfo = Generator_->KeyInfo();
    YT_LOG_INFO(
        "Rotating keypair (CurrentKeyPair: %v)",
        (currentKeyInfo ? std::optional(GetKeyId(currentKeyInfo->Meta())) : std::nullopt));

    auto now = Now();
    auto newKeyId = TGuid::Create();
    auto config = Config_.Acquire();
    auto newKeyPair = New<TKeyPair>(TKeyPairMetadataImpl<TKeyPairVersion{0, 1}>{
        .OwnerId = KeyWriter_->GetOwner(),
        .KeyId = TKeyId(newKeyId),
        .CreatedAt = now,
        .ValidAfter = now - config->TimeSyncMargin,
        .ExpiresAt = now + config->KeyExpirationDelta,
    });

    auto keyInfo = newKeyPair->KeyInfo();

    auto error = WaitFor(KeyWriter_->RegisterKey(keyInfo));
    if (!error.IsOK()) {
       YT_LOG_ERROR(error, "Failed to register new keypair during rotation (NewKeyPair: %v)", GetKeyId(keyInfo->Meta()));
       return error;
    }

    Generator_->SetKeyPair(std::move(newKeyPair));

    YT_LOG_INFO("Rotated keypair (NewKeyPair: %v)", GetKeyId(keyInfo->Meta()));
    return {};
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TKeyRotator::GetNextRotationFuture()
{
    return Executor_->GetExecutedEvent();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

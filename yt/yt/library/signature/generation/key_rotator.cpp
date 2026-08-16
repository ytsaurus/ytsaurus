#include "key_rotator.h"

#include "config.h"
#include "signature_generator.h"

#include <yt/yt/library/signature/common/key_store.h>
#include <yt/yt/library/signature/common/private.h>

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
    YT_TLOG_INFO("Key rotator initialized")
        .With("KeyRotationInterval", Config_.Acquire()->KeyRotationOptions.Period);
}


TFuture<void> TKeyRotator::Start()
{
    YT_TLOG_DEBUG("Starting key rotation");
    return Executor_->StartAndGetFirstExecutedEvent();
}

TFuture<void> TKeyRotator::Stop()
{
    YT_TLOG_DEBUG("Stopping key rotation");
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
    YT_TLOG_INFO("Key rotator reconfigured")
        .With("KeyRotationInterval", keyRotationOptions.Period)
        .With("Splay", keyRotationOptions.Splay)
        .With("Jitter", keyRotationOptions.Jitter);
}

////////////////////////////////////////////////////////////////////////////////

TError TKeyRotator::DoRotate()
{
    auto currentKeyInfo = Generator_->KeyInfo();
    YT_TLOG_INFO("Rotating keypair")
        .With("CurrentKeyPair", (currentKeyInfo ? std::optional(GetKeyId(currentKeyInfo->Meta())) : std::nullopt));

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
        auto [minBackoff, maxBackoff] = Executor_->GetBackoffInterval();
        YT_TLOG_ERROR("Failed to register new keypair during rotation")
            .With("NewKeyPair", GetKeyId(keyInfo->Meta()))
            .WithFormat("BackoffTime", "[%v, %v]", minBackoff, maxBackoff)
            .With(error);
        return error;
    }

    Generator_->SetKeyPair(std::move(newKeyPair));

    YT_TLOG_INFO("Rotated keypair")
        .With("NewKeyPair", GetKeyId(keyInfo->Meta()));
    return {};
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TKeyRotator::GetNextRotationFuture()
{
    return Executor_->GetExecutedEvent();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

#include "key_rotator.h"

#include "config.h"
#include "key_store.h"
#include "private.h"
#include "signature_generator.h"

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
    , Executor_(New<TPeriodicExecutor>(
        std::move(invoker),
        BIND_NO_PROPAGATE(&TKeyRotator::DoRotate, MakeWeak(this)),
        Config_->KeyRotationInterval))
{
    InitializeCryptography();
    YT_LOG_INFO("Key rotator initialized (KeyRotationInterval %v)", Config_->KeyRotationInterval);
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

////////////////////////////////////////////////////////////////////////////////

void TKeyRotator::DoRotate()
{
    auto currentKeyInfo = Generator_->KeyInfo();
    YT_LOG_INFO(
        "Rotating keypair (CurrentKeyPair: %v)",
        (currentKeyInfo ? std::optional(GetKeyId(currentKeyInfo->Meta())) : std::nullopt));

    auto now = Now();
    auto newKeyId = TGuid::Create();
    auto newKeyPair = New<TKeyPair>(TKeyPairMetadataImpl<TKeyPairVersion{0, 1}>{
        .OwnerId = KeyWriter_->GetOwner(),
        .KeyId = TKeyId(newKeyId),
        .CreatedAt = now,
        .ValidAfter = now - Config_->TimeSyncMargin,
        .ExpiresAt = now + Config_->KeyExpirationDelta,
    });

    auto keyInfo = newKeyPair->KeyInfo();

    WaitFor(KeyWriter_->RegisterKey(keyInfo))
        .ThrowOnError();

    Generator_->SetKeyPair(std::move(newKeyPair));

    YT_LOG_INFO("Rotated keypair (NewKeyPair: %v)", GetKeyId(keyInfo->Meta()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

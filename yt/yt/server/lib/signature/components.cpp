#include "components.h"

#include "config.h"
#include "cypress_key_store.h"
#include "key_rotator.h"
#include "private.h"
#include "signature_generator.h"
#include "signature_validator.h"

#include <yt/yt/client/signature/dynamic.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TSignatureComponents::TSignatureComponents(
    const TSignatureComponentsConfigPtr& config,
    TOwnerId ownerId,
    const IConnectionPtr& connection,
    IInvokerPtr rotateInvoker)
    : OwnerId_(std::move(ownerId))
    , Client_(connection->CreateNativeClient(
        config->UseRootUser
            ? TClientOptions::Root()
            : TClientOptions::FromUser(NSecurityClient::SignatureKeysmithUserName)))
    , RotateInvoker_(std::move(rotateInvoker))
    , CypressKeyReader_(config->Validation
        ? New<TCypressKeyReader>(config->Validation->CypressKeyReader, Client_)
        : nullptr)
    , UnderlyingValidator_(config->Validation
        ? New<TSignatureValidator>(CypressKeyReader_)
        : nullptr)
    , DynamicSignatureValidator_(New<TDynamicSignatureValidator>(
        UnderlyingValidator_ ? UnderlyingValidator_ : CreateAlwaysThrowingSignatureValidator()))
    , CypressKeyWriter_(config->Generation
        ? New<TCypressKeyWriter>(config->Generation->CypressKeyWriter, OwnerId_, Client_)
        : nullptr)
    , UnderlyingGenerator_(config->Generation
        ? New<TSignatureGenerator>(config->Generation->Generator)
        : nullptr)
    , KeyRotator_(config->Generation
        ? New<TKeyRotator>(config->Generation->KeyRotator, RotateInvoker_, CypressKeyWriter_, UnderlyingGenerator_)
        : nullptr)
    , DynamicSignatureGenerator_(New<TDynamicSignatureGenerator>(
        UnderlyingGenerator_ ? UnderlyingGenerator_ : CreateAlwaysThrowingSignatureGenerator()))
{
    InitializeCryptographyIfRequired(config);
}

void TSignatureComponents::InitializeCryptographyIfRequired(const TSignatureComponentsConfigPtr& config)
{
    bool isInitializationRequired = (config->Validation || config->Generation) && !(InitializeCryptographyFuture_);
    if (!isInitializationRequired) {
        return;
    }

    auto actionQueue = New<TActionQueue>("CryptoInit");
    auto invoker = actionQueue->GetInvoker();

    // NB: destroy actionQueue upon completing initialization.
    InitializeCryptographyFuture_ = InitializeCryptography(invoker)
        .Apply(BIND([actionQueue = std::move(actionQueue)] {}));
}

TFuture<void> TSignatureComponents::Reconfigure(const TSignatureComponentsConfigPtr& config) {
    YT_LOG_INFO("Reconfiguring signature components");

    auto guard = Guard(ReconfigureSpinLock_);
    TForbidContextSwitchGuard contextSwitchGuard;

    auto returnFuture = OKFuture;

    InitializeCryptographyIfRequired(config);
    if (config->Generation) {
        if (CypressKeyWriter_) {
            CypressKeyWriter_->Reconfigure(config->Generation->CypressKeyWriter);
        } else {
            CypressKeyWriter_ = New<TCypressKeyWriter>(config->Generation->CypressKeyWriter, OwnerId_, Client_);
        }

        if (UnderlyingGenerator_) {
            UnderlyingGenerator_->Reconfigure(config->Generation->Generator);
        } else {
            UnderlyingGenerator_ = New<TSignatureGenerator>(config->Generation->Generator);
        }

        if (KeyRotator_) {
            // NB: Best effort attempt to get the first rotation *after* Reconfigure.
            // Can't be put later, because Reconfigure might trigger an immediate rotation.
            returnFuture = KeyRotator_->GetNextRotation();
            KeyRotator_->Reconfigure(config->Generation->KeyRotator);
        } else {
            KeyRotator_ = New<TKeyRotator>(config->Generation->KeyRotator, RotateInvoker_, CypressKeyWriter_, UnderlyingGenerator_);
            // NB: We can't wait for anything in Reconfigure.
            returnFuture = DoStartRotation();
        }

        DynamicSignatureGenerator_->SetUnderlying(UnderlyingGenerator_);
    } else {
        DynamicSignatureGenerator_->SetUnderlying(CreateAlwaysThrowingSignatureGenerator());
        UnderlyingGenerator_.Reset();

        if (KeyRotator_) {
            // NB: We can't wait for anything in Reconfigure.
            returnFuture = KeyRotator_->Stop();
        }
        KeyRotator_.Reset();

        CypressKeyWriter_.Reset();
    }

    if (config->Validation) {
        if (CypressKeyReader_) {
            CypressKeyReader_->Reconfigure(config->Validation->CypressKeyReader);
        } else {
            CypressKeyReader_ = New<TCypressKeyReader>(config->Validation->CypressKeyReader, Client_);
        }

        if (!UnderlyingValidator_) {
            UnderlyingValidator_ = New<TSignatureValidator>(CypressKeyReader_);
        }

        DynamicSignatureValidator_->SetUnderlying(UnderlyingValidator_);
    } else {
        DynamicSignatureValidator_->SetUnderlying(CreateAlwaysThrowingSignatureValidator());
        UnderlyingValidator_.Reset();
        CypressKeyReader_.Reset();
    }

    return returnFuture;
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TSignatureComponents::StartRotation()
{
    auto guard = Guard(ReconfigureSpinLock_);
    return DoStartRotation();
}

TFuture<void> TSignatureComponents::DoStartRotation() const
{
    YT_ASSERT_SPINLOCK_AFFINITY(ReconfigureSpinLock_);

    if (KeyRotator_) {
        return InitializeCryptographyFuture_.Apply(
            BIND([weakRotator = MakeWeak(KeyRotator_)] {
                if (auto keyRotator = weakRotator.Lock()) {
                    return keyRotator->Start();
                }
                return OKFuture;
            }));
    }
    return OKFuture;
}

TFuture<void> TSignatureComponents::StopRotation()
{
    auto guard = Guard(ReconfigureSpinLock_);
    return KeyRotator_ ? KeyRotator_->Stop() : OKFuture;
}

TFuture<void> TSignatureComponents::DoRotateOutOfBand() const
{
    YT_ASSERT_SPINLOCK_AFFINITY(ReconfigureSpinLock_);

    if (KeyRotator_) {
        return InitializeCryptographyFuture_.Apply(
            BIND([weakRotator = MakeWeak(KeyRotator_)] {
                if (auto keyRotator = weakRotator.Lock()) {
                    return keyRotator->Rotate();
                }
                return OKFuture;
            }));
    }
    return OKFuture;
}

TFuture<void> TSignatureComponents::RotateOutOfBand()
{
    auto guard = Guard(ReconfigureSpinLock_);
    return DoRotateOutOfBand();
}

////////////////////////////////////////////////////////////////////////////////

ISignatureGeneratorPtr TSignatureComponents::GetSignatureGenerator()
{
    return DynamicSignatureGenerator_;
}

ISignatureValidatorPtr TSignatureComponents::GetSignatureValidator()
{
    return DynamicSignatureValidator_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

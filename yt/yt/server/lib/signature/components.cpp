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

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NThreading;
using namespace NYTree;

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
    , AppliedKeyReaderConfigNode_(config->Validation && config->Validation->Enabled
        ? ConvertToNode(config->Validation->CypressKeyReader)
        : nullptr)
    , CypressKeyReader_(config->Validation && config->Validation->Enabled
        ? New<TCypressKeyReader>(config->Validation->CypressKeyReader, Client_)
        : nullptr)
    , UnderlyingValidator_(config->Validation && config->Validation->Enabled
        ? New<TSignatureValidator>(CypressKeyReader_)
        : nullptr)
    , DynamicSignatureValidator_(New<TDynamicSignatureValidator>(
        UnderlyingValidator_ ? UnderlyingValidator_ : CreateAlwaysThrowingSignatureValidator()))
    , CypressKeyWriter_(config->Generation && config->Generation->Enabled
        ? New<TCypressKeyWriter>(config->Generation->CypressKeyWriter, OwnerId_, Client_)
        : nullptr)
    , UnderlyingGenerator_(config->Generation && config->Generation->Enabled
        ? New<TSignatureGenerator>(config->Generation->Generator)
        : nullptr)
    , KeyRotator_(config->Generation && config->Generation->Enabled
        ? New<TKeyRotator>(config->Generation->KeyRotator, RotateInvoker_, CypressKeyWriter_, UnderlyingGenerator_)
        : nullptr)
    , DynamicSignatureGenerator_(New<TDynamicSignatureGenerator>(
        UnderlyingGenerator_ ? UnderlyingGenerator_ : CreateAlwaysThrowingSignatureGenerator()))
{
    InitializeCryptographyIfRequired(config);
}

void TSignatureComponents::InitializeCryptographyIfRequired(const TSignatureComponentsConfigPtr& config)
{
    bool validationEnabled = config->Validation && config->Validation->Enabled;
    bool generationEnabled = config->Generation && config->Generation->Enabled;
    bool isInitializationRequired = (validationEnabled || generationEnabled) && !InitializeCryptographyFuture_;
    if (!isInitializationRequired) {
        return;
    }

    auto actionQueue = New<TActionQueue>("CryptoInit");
    auto invoker = actionQueue->GetInvoker();

    // NB: destroy actionQueue upon completing initialization.
    InitializeCryptographyFuture_ = InitializeCryptography(invoker)
        .Apply(BIND([actionQueue = std::move(actionQueue)] {}));
}

TFuture<void> TSignatureComponents::Reconfigure(const TSignatureComponentsConfigPtr& config)
{
    YT_LOG_INFO("Reconfiguring signature components");

    auto returnFuture = OKFuture;
    TKeyRotatorPtr keyRotatorToStop;
    {
        auto guard = Guard(ReconfigureSpinLock_);
        TForbidContextSwitchGuard contextSwitchGuard;

        InitializeCryptographyIfRequired(config);
        if (config->Generation && config->Generation->Enabled) {
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

            keyRotatorToStop = std::move(KeyRotator_);

            CypressKeyWriter_.Reset();
        }

        if (config->Validation && config->Validation->Enabled) {
            auto newKeyReaderConfigNode = ConvertToNode(config->Validation->CypressKeyReader);
            if (CypressKeyReader_) {
                // NB: Reconfiguring the reader drops its key cache, so it is only done
                // when the reader config has actually changed.
                if (!AreNodesEqual(AppliedKeyReaderConfigNode_, newKeyReaderConfigNode)) {
                    CypressKeyReader_->Reconfigure(config->Validation->CypressKeyReader);
                }
            } else {
                CypressKeyReader_ = New<TCypressKeyReader>(config->Validation->CypressKeyReader, Client_);
                UnderlyingValidator_ = New<TSignatureValidator>(CypressKeyReader_);
            }
            AppliedKeyReaderConfigNode_ = std::move(newKeyReaderConfigNode);

            DynamicSignatureValidator_->SetUnderlying(UnderlyingValidator_);
        } else {
            DynamicSignatureValidator_->SetUnderlying(CreateAlwaysThrowingSignatureValidator());
            UnderlyingValidator_.Reset();
            CypressKeyReader_.Reset();
            AppliedKeyReaderConfigNode_.Reset();
        }
    }

    if (keyRotatorToStop) {
        return keyRotatorToStop->Stop();
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
    TKeyRotatorPtr keyRotator;
    {
        auto guard = Guard(ReconfigureSpinLock_);
        keyRotator = KeyRotator_;
    }
    return keyRotator ? keyRotator->Stop() : OKFuture;
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

#pragma once

#include "public.h"

#include <yt/yt/library/signature/common/public.h>

#include <yt/yt/library/signature/generation/public.h>

#include <yt/yt/library/signature/validation/public.h>

#include <yt/yt/client/api/public.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

/*!
 * \note Thread affinity: any
 */
class TSignatureComponents final
{
public:
    TSignatureComponents(
        const TSignatureComponentsConfigPtr& config,
        TOwnerId ownerId,
        const NApi::IConnectionPtr& connection,
        IInvokerPtr rotateInvoker);

    TFuture<void> StartRotation();

    TFuture<void> StopRotation();

    TFuture<void> RotateOutOfBand();

    ISignatureValidatorPtr GetSignatureValidator();
    ISignatureGeneratorPtr GetSignatureGenerator();

    TFuture<void> Reconfigure(const TSignatureComponentsConfigPtr& config);

private:
    const TOwnerId OwnerId_;
    const NApi::IClientPtr Client_;
    const IInvokerPtr RotateInvoker_;

    TFuture<void> InitializeCryptographyFuture_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ReconfigureSpinLock_);

    TCypressKeyReaderPtr CypressKeyReader_;
    TSignatureValidatorPtr UnderlyingValidator_;
    const TDynamicSignatureValidatorPtr DynamicSignatureValidator_;

    TCypressKeyWriterPtr CypressKeyWriter_;
    TSignatureGeneratorPtr UnderlyingGenerator_;
    TKeyRotatorPtr KeyRotator_;
    const TDynamicSignatureGeneratorPtr DynamicSignatureGenerator_;

    void InitializeCryptographyIfRequired(const TSignatureComponentsConfigPtr& config);

    TFuture<void> DoStartRotation() const;

    TFuture<void> DoRotateOutOfBand() const;
};

DEFINE_REFCOUNTED_TYPE(TSignatureComponents)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

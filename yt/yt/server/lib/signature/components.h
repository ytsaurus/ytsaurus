#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/ytree/public.h>

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
        const NApi::NNative::IConnectionPtr& connection,
        IInvokerPtr rotateInvoker);

    TFuture<void> StartRotation();

    TFuture<void> StopRotation();

    TFuture<void> RotateOutOfBand();

    ISignatureValidatorPtr GetSignatureValidator();
    ISignatureGeneratorPtr GetSignatureGenerator();

    TFuture<void> Reconfigure(const TSignatureComponentsConfigPtr& config);

private:
    const TOwnerId OwnerId_;
    const NApi::NNative::IClientPtr Client_;
    const IInvokerPtr RotateInvoker_;

    TFuture<void> InitializeCryptographyFuture_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ReconfigureSpinLock_);

    //! A YSON snapshot of the last applied key reader config, used to detect
    //! config changes; null iff validation is disabled. A snapshot is required
    //! since callers may mutate the config object in place.
    //! Guarded by ReconfigureSpinLock_.
    NYTree::INodePtr AppliedKeyReaderConfigNode_;

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

#pragma once

#include "public.h"

#include <yt/yt/client/signature/generator.h>
#include <yt/yt/client/signature/validator.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class TDynamicSignatureGenerator
    : public ISignatureGenerator
{
public:
    TDynamicSignatureGenerator(ISignatureGeneratorPtr underlying);

    /*!
    *  \note Thread affinity: any
    */
    void SetUnderlying(ISignatureGeneratorPtr underlying);

    /*!
    *  \note Thread affinity: any
    */
    void Resign(const TSignaturePtr& signature) const final;

private:
    TAtomicIntrusivePtr<ISignatureGenerator> Underlying_;
};

DEFINE_REFCOUNTED_TYPE(TDynamicSignatureGenerator)

////////////////////////////////////////////////////////////////////////////////

class TDynamicSignatureValidator
    : public ISignatureValidator
{
public:
    TDynamicSignatureValidator(ISignatureValidatorPtr underlying);

    /*!
    *  \note Thread affinity: any
    */
    void SetUnderlying(ISignatureValidatorPtr underlying);

    /*!
    *  \note Thread affinity: any
    */
    TFuture<bool> Validate(const TSignaturePtr& signature) const final;

private:
    TAtomicIntrusivePtr<ISignatureValidator> Underlying_;
};

DEFINE_REFCOUNTED_TYPE(TDynamicSignatureValidator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

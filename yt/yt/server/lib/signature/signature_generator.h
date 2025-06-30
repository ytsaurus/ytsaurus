#pragma once

#include "public.h"

#include "key_pair.h"

#include <yt/yt/client/signature/generator.h>

#include <yt/yt/core/yson/public.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class TSignatureGenerator
    : public ISignatureGenerator
{
public:
    explicit TSignatureGenerator(TSignatureGeneratorConfigPtr config);

    /*!
     *  \note Thread affinity: any
    */
    [[nodiscard]] TKeyInfoPtr KeyInfo() const;

    /*!
    *  \note Thread affinity: any
    */
    void SetKeyPair(TKeyPairPtr keyPair);

private:
    const TSignatureGeneratorConfigPtr Config_;
    TAtomicIntrusivePtr<TKeyPair> KeyPair_;

    /*!
    *  \note Thread affinity: any
    */
    void DoSign(const TSignaturePtr& signature) const final;
};

DEFINE_REFCOUNTED_TYPE(TSignatureGenerator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

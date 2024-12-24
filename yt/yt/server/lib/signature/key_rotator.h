#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class TKeyRotator final
{
public:
    TKeyRotator(
        TKeyRotatorConfigPtr config,
        IInvokerPtr invoker,
        TSignatureGeneratorPtr generator);

    void Start();

private:
    const TKeyRotatorConfigPtr Config_;
    const NConcurrency::TPeriodicExecutorPtr Executor_;
};

DEFINE_REFCOUNTED_TYPE(TKeyRotator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

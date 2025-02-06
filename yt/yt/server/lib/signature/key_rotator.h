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

    //! Starts key rotation, waiting for the completion of the first rotation.
    TFuture<void> Start();

    //! Stops key rotation, waiting for the completion of the current rotation.
    TFuture<void> Stop();

private:
    const TKeyRotatorConfigPtr Config_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    const NConcurrency::TPeriodicExecutorPtr Executor_;
};

DEFINE_REFCOUNTED_TYPE(TKeyRotator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

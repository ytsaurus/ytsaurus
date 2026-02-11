#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

/*!
* \note Thread affinity: any
*/
class TKeyRotator final
{
public:
    TKeyRotator(
        TKeyRotatorConfigPtr config,
        IInvokerPtr invoker,
        IKeyStoreWriterPtr keyWriter,
        TSignatureGeneratorPtr generator);

    //! Starts periodic key rotation, waiting for the completion of the first rotation.
    TFuture<void> Start();

    //! Stops periodic key rotation, waiting for the completion of the current rotation.
    TFuture<void> Stop();

    //! Schedules an out-of-order key rotation.
    TFuture<void> Rotate();

    //! Returns the future that is set when the next started rotation completes, with "next"
    //! starting from some point inside of this call.
    TFuture<void> GetNextRotationFuture();

    void Reconfigure(TKeyRotatorConfigPtr config);

private:
    TAtomicIntrusivePtr<TKeyRotatorConfig> Config_;
    const IKeyStoreWriterPtr KeyWriter_;
    const TSignatureGeneratorPtr Generator_;
    const NConcurrency::TRetryingPeriodicExecutorPtr Executor_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ReconfigureSpinLock_);

    TError DoRotate();
};

DEFINE_REFCOUNTED_TYPE(TKeyRotator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

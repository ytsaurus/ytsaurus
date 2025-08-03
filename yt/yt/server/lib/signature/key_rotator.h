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
        IKeyStoreWriterPtr keyWriter,
        TSignatureGeneratorPtr generator);

    //! Starts periodic key rotation, waiting for the completion of the first rotation.
    /*!
    *  \note Thread affinity: any
    */
    TFuture<void> Start();

    //! Stops periodic key rotation, waiting for the completion of the current rotation.
    /*!
    *  \note Thread affinity: any
    */
    TFuture<void> Stop();

    //! Schedules an out-of-order key rotation.
    /*!
    *  \note Thread affinity: any
    */
    TFuture<void> Rotate();

private:
    const TKeyRotatorConfigPtr Config_;
    const IKeyStoreWriterPtr KeyWriter_;
    const TSignatureGeneratorPtr Generator_;
    const NConcurrency::TPeriodicExecutorPtr Executor_;

    void DoRotate();
};

DEFINE_REFCOUNTED_TYPE(TKeyRotator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

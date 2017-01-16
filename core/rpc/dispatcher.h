#pragma once

#include "public.h"

#include <yt/core/actions/public.h>

#include <yt/core/misc/shutdownable.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TDispatcher
    : public IShutdownable
{
public:
    TDispatcher();

    ~TDispatcher();

    static TDispatcher* Get();

    static void StaticShutdown();

    void Configure(int poolSize);

    virtual void Shutdown() override;

    //! Returns the invoker for the single thread used to dispatch light callbacks
    //! (e.g. discovery or request cancelation).
    const IInvokerPtr& GetLightInvoker();
    //! Returns the invoker for the thread pool used to dispatch heavy callbacks
    //! (e.g. serialization).
    const IInvokerPtr& GetHeavyInvoker();

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

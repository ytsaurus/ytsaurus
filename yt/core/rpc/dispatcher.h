#pragma once

#include "public.h"

#include <core/misc/shutdownable.h>

#include <core/actions/public.h>

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

    void Configure(int poolSize);

    virtual void Shutdown() override;

    /*!
     * This invoker is used by RPC to dispatch callbacks.
     */
    IInvokerPtr GetPoolInvoker();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

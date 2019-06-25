#pragma once

#include "public.h"

#include <yt/core/actions/public.h>

#include <yt/core/misc/shutdownable.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

// XXX(sandello): Make this an interface with singleton implementation?
class TDispatcher
    : public IShutdownable
{
public:
    TDispatcher();

    ~TDispatcher();

    static TDispatcher* Get();

    static void StaticShutdown();

    void Configure(TDispatcherConfigPtr config);

    virtual void Shutdown() override;

    IInvokerPtr GetReaderInvoker();
    IInvokerPtr GetWriterInvoker();

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient


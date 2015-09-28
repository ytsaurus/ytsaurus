#pragma once

#include "public.h"

#include <core/misc/shutdownable.h>

#include <core/actions/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

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
    IInvokerPtr GetCompressionPoolInvoker();
    IInvokerPtr GetErasurePoolInvoker();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT


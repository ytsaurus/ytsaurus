#include "dispatcher.h"
#include "config.h"

#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/action_queue.h>

#include <yt/core/misc/singleton.h>
#include <yt/core/misc/shutdown.h>

#include <yt/core/rpc/dispatcher.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TDispatcher::TImpl
{
public:
    TImpl()
        : ReaderMemoryManagerInvoker_(CreateSerializedInvoker(GetReaderInvoker()))
    { }

    void Configure(TDispatcherConfigPtr config)
    {
        ReaderThreadPool_->Configure(config->ChunkReaderPoolSize);
    }

    void Shutdown()
    {
        ReaderThreadPool_->Shutdown();
        WriterThread_->Shutdown();
    }

    IInvokerPtr GetReaderInvoker()
    {
        return ReaderThreadPool_->GetInvoker();
    }

    IInvokerPtr GetWriterInvoker()
    {
        return WriterThread_->GetInvoker();
    }

    const IInvokerPtr& GetReaderMemoryManagerInvoker()
    {
        return ReaderMemoryManagerInvoker_;
    }

private:
    const TActionQueuePtr WriterThread_ = New<TActionQueue>("ChunkWriter");
    const TThreadPoolPtr ReaderThreadPool_ = New<TThreadPool>(TDispatcherConfig::DefaultChunkReaderPoolSize, "ChunkReader");
    const IInvokerPtr ReaderMemoryManagerInvoker_;
};

/////////////////////////////////////////////////////////////////////////////

TDispatcher::TDispatcher()
    : Impl_(new TImpl())
{ }

TDispatcher::~TDispatcher()
{ }

TDispatcher* TDispatcher::Get()
{
    return Singleton<TDispatcher>();
}

void TDispatcher::StaticShutdown()
{
    Get()->Shutdown();
}

void TDispatcher::Configure(TDispatcherConfigPtr config)
{
    Impl_->Configure(std::move(config));
}

void TDispatcher::Shutdown()
{
    Impl_->Shutdown();
}

IInvokerPtr TDispatcher::GetReaderInvoker()
{
    return Impl_->GetReaderInvoker();
}

IInvokerPtr TDispatcher::GetWriterInvoker()
{
    return Impl_->GetWriterInvoker();
}

const IInvokerPtr& TDispatcher::GetReaderMemoryManagerInvoker()
{
    return Impl_->GetReaderMemoryManagerInvoker();
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_SHUTDOWN_CALLBACK(9, TDispatcher::StaticShutdown);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

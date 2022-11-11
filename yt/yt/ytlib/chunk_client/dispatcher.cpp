#include "dispatcher.h"
#include "config.h"

#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/singleton.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TDispatcher::TImpl
{
public:
    void Configure(TDispatcherConfigPtr config)
    {
        ReaderThreadPool_->Configure(config->ChunkReaderPoolSize);
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
        return MemoryManagerThread_->GetInvoker();
    }

private:
    const TActionQueuePtr WriterThread_ = New<TActionQueue>("ChunkWriter");
    const IThreadPoolPtr ReaderThreadPool_ = CreateThreadPool(TDispatcherConfig::DefaultChunkReaderPoolSize, "ChunkReader");
    const TActionQueuePtr MemoryManagerThread_ = New<TActionQueue>("MemoryManager");
};

/////////////////////////////////////////////////////////////////////////////

TDispatcher::TDispatcher()
    : Impl_(new TImpl())
{ }

TDispatcher::~TDispatcher() = default;

TDispatcher* TDispatcher::Get()
{
    return Singleton<TDispatcher>();
}

void TDispatcher::Configure(TDispatcherConfigPtr config)
{
    Impl_->Configure(std::move(config));
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

} // namespace NYT::NChunkClient

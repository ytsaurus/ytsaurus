#include "dispatcher.h"
#include "config.h"

#include <core/misc/lazy_ptr.h>
#include <core/misc/singleton.h>

#include <core/concurrency/action_queue.h>
#include <core/concurrency/thread_pool.h>

namespace NYT {
namespace NChunkClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TDispatcher::TImpl
{
public:
    TImpl()
        : CompressionPoolInvoker_(BIND(&TImpl::CreateCompressionPoolInvoker, Unretained(this)))
        , ErasurePoolInvoker_(BIND(&TImpl::CreateErasurePoolInvoker, Unretained(this)))
    { }

    void Configure(TDispatcherConfigPtr config)
    {
        CompressionPool_->Configure(config->CompressionPoolSize);
        ErasurePool_->Configure(config->ErasurePoolSize);
    }

    void Shutdown()
    {
        ReaderThread_->Shutdown();
        WriterThread_->Shutdown();
        CompressionPool_->Shutdown();
        ErasurePool_->Shutdown();
    }

    IInvokerPtr GetReaderInvoker()
    {
        return ReaderThread_->GetInvoker();
    }

    IInvokerPtr GetWriterInvoker()
    {
        return WriterThread_->GetInvoker();
    }

    IPrioritizedInvokerPtr GetCompressionPoolInvoker()
    {
        return CompressionPoolInvoker_.Get();
    }

    IPrioritizedInvokerPtr GetErasurePoolInvoker()
    {
        return ErasurePoolInvoker_.Get();
    }

private:
    TActionQueuePtr ReaderThread_ = New<TActionQueue>("ChunkReader");
    TActionQueuePtr WriterThread_ = New<TActionQueue>("ChunkWriter");
    TThreadPoolPtr CompressionPool_ = New<TThreadPool>(4, "Compression");
    TThreadPoolPtr ErasurePool_ = New<TThreadPool>(4, "Erasure");
    TLazyIntrusivePtr<IPrioritizedInvoker> CompressionPoolInvoker_;
    TLazyIntrusivePtr<IPrioritizedInvoker> ErasurePoolInvoker_;

    IPrioritizedInvokerPtr CreateCompressionPoolInvoker()
    {
        return CreatePrioritizedInvoker(CompressionPool_->GetInvoker());
    }

    IPrioritizedInvokerPtr CreateErasurePoolInvoker()
    {
        return CreatePrioritizedInvoker(ErasurePool_->GetInvoker());
    }
};

TDispatcher::TDispatcher()
    : Impl_(new TImpl())
{ }

TDispatcher::~TDispatcher()
{ }

TDispatcher* TDispatcher::Get()
{
    return TSingletonWithFlag<TDispatcher>::Get();
}

void TDispatcher::StaticShutdown()
{
    if (TSingletonWithFlag<TDispatcher>::WasCreated()) {
        TSingletonWithFlag<TDispatcher>::Get()->Shutdown();
    }
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

IPrioritizedInvokerPtr TDispatcher::GetCompressionPoolInvoker()
{
    return Impl_->GetCompressionPoolInvoker();
}

IPrioritizedInvokerPtr TDispatcher::GetErasurePoolInvoker()
{
    return Impl_->GetErasurePoolInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

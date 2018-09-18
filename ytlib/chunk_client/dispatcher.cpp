#include "dispatcher.h"
#include "config.h"

#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/action_queue.h>

#include <yt/core/misc/lazy_ptr.h>
#include <yt/core/misc/singleton.h>
#include <yt/core/misc/shutdown.h>

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

    IPrioritizedInvokerPtr GetPrioritizedCompressionPoolInvoker()
    {
        return CompressionPoolInvoker_.Get();
    }

    IPrioritizedInvokerPtr GetPrioritizedErasurePoolInvoker()
    {
        return ErasurePoolInvoker_.Get();
    }

    IInvokerPtr GetCompressionPoolInvoker()
    {
        return CompressionPool_->GetInvoker();
    }

private:
    const TActionQueuePtr ReaderThread_ = New<TActionQueue>("ChunkReader");
    const TActionQueuePtr WriterThread_ = New<TActionQueue>("ChunkWriter");
    const TThreadPoolPtr CompressionPool_ = New<TThreadPool>(4, "Compression");
    const TThreadPoolPtr ErasurePool_ = New<TThreadPool>(4, "Erasure");
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

IPrioritizedInvokerPtr TDispatcher::GetPrioritizedCompressionPoolInvoker()
{
    return Impl_->GetPrioritizedCompressionPoolInvoker();
}

IPrioritizedInvokerPtr TDispatcher::GetPrioritizedErasurePoolInvoker()
{
    return Impl_->GetPrioritizedErasurePoolInvoker();
}

IInvokerPtr TDispatcher::GetCompressionPoolInvoker()
{
    return Impl_->GetCompressionPoolInvoker();
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_SHUTDOWN_CALLBACK(9, TDispatcher::StaticShutdown);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

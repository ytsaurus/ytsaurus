#include "dispatcher.h"
#include "config.h"

#include <core/misc/lazy_ptr.h>
#include <core/misc/singleton.h>

#include <core/concurrency/action_queue.h>

namespace NYT {
namespace NChunkClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TDispatcher::TImpl
{
public:
    TImpl()
        : ReaderThread_(TActionQueue::CreateFactory("ChunkReader"))
        , WriterThread_(TActionQueue::CreateFactory("ChunkWriter"))
        , CompressionPool_(BIND(
            New<TThreadPool, const int&, const Stroka&>,
            ConstRef(CompressionPoolSize_),
            "Compression"))
        , ErasurePool_(BIND(
            New<TThreadPool, const int&, const Stroka&>,
            ConstRef(ErasurePoolSize_),
            "Erasure"))
        , CompressionPoolInvoker_(BIND(&TImpl::CreateCompressionPoolInvoker, Unretained(this)))
        , ErasurePoolInvoker_(BIND(&TImpl::CreateErasurePoolInvoker, Unretained(this)))
    { }

    void Configure(TDispatcherConfigPtr config)
    {
        // We believe in proper memory ordering here.
        YCHECK(!CompressionPool_.HasValue());
        // We do not really want to store entire config within us.
        CompressionPoolSize_ = config->CompressionPoolSize;
        // This is not redundant, since the check and the assignment above are
        // not atomic and (adversary) thread can initialize thread pool in parallel.
        YCHECK(!CompressionPool_.HasValue());

        // We believe in proper memory ordering here.
        YCHECK(!ErasurePool_.HasValue());
        // We do not really want to store entire config within us.
        ErasurePoolSize_ = config->ErasurePoolSize;
        // This is not redundant, since the check and the assignment above are
        // not atomic and (adversary) thread can initialize thread pool in parallel.
        YCHECK(!ErasurePool_.HasValue());
    }

    void Shutdown()
    {
        if (ReaderThread_.HasValue()) {
            ReaderThread_->Shutdown();
        }

        if (WriterThread_.HasValue()) {
            WriterThread_->Shutdown();
        }

        if (CompressionPool_.HasValue()) {
            CompressionPool_->Shutdown();
        }

        if (ErasurePool_.HasValue()) {
            ErasurePool_->Shutdown();
        }
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
    int CompressionPoolSize_ = 4;
    int ErasurePoolSize_ = 4;

    TLazyIntrusivePtr<NConcurrency::TActionQueue> ReaderThread_;
    TLazyIntrusivePtr<NConcurrency::TActionQueue> WriterThread_;
    TLazyIntrusivePtr<NConcurrency::TThreadPool> CompressionPool_;
    TLazyIntrusivePtr<NConcurrency::TThreadPool> ErasurePool_;
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

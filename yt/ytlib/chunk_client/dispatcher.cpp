#include "config.h"
#include "dispatcher.h"

namespace NYT {
namespace NChunkClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TDispatcher::TDispatcher()
    : CompressionPoolSize_(4)
    , ErasurePoolSize_(4)
    , ReaderThread_(
        TActionQueue::CreateFactory("ChunkReader"))
    , WriterThread_(
        TActionQueue::CreateFactory("ChunkWriter"))
    , CompressionThreadPool_(BIND(
        NYT::New<TThreadPool, const int&, const Stroka&>,
        ConstRef(CompressionPoolSize_),
        "Compression"))
    , ErasureThreadPool_(BIND(
        NYT::New<TThreadPool, const int&, const Stroka&>,
        ConstRef(ErasurePoolSize_),
        "Erasure"))
{ }

TDispatcher* TDispatcher::Get()
{
    return Singleton<TDispatcher>();
}

void TDispatcher::Configure(TDispatcherConfigPtr config)
{
    // We believe in proper memory ordering here.
    YCHECK(!CompressionThreadPool_.HasValue());
    // We do not really want to store entire config within us.
    CompressionPoolSize_ = config->CompressionPoolSize;
    // This is not redundant, since the check and the assignment above are
    // not atomic and (adversary) thread can initialize thread pool in parallel.
    YCHECK(!CompressionThreadPool_.HasValue());

    // We believe in proper memory ordering here.
    YCHECK(!ErasureThreadPool_.HasValue());
    // We do not really want to store entire config within us.
    ErasurePoolSize_ = config->ErasurePoolSize;
    // This is not redundant, since the check and the assignment above are
    // not atomic and (adversary) thread can initialize thread pool in parallel.
    YCHECK(!ErasureThreadPool_.HasValue());
}

IInvokerPtr TDispatcher::GetReaderInvoker()
{
    return ReaderThread_->GetInvoker();
}

IInvokerPtr TDispatcher::GetWriterInvoker()
{
    return WriterThread_->GetInvoker();
}

IInvokerPtr TDispatcher::GetCompressionInvoker()
{
    return CompressionThreadPool_->GetInvoker();
}

IInvokerPtr TDispatcher::GetErasureInvoker()
{
    return ErasureThreadPool_->GetInvoker();
}

void TDispatcher::Shutdown()
{
    if (ReaderThread_.HasValue()) {
        ReaderThread_->Shutdown();
    }

    if (WriterThread_.HasValue()) {
        WriterThread_->Shutdown();
    }

    if (CompressionThreadPool_.HasValue()) {
        CompressionThreadPool_->Shutdown();
    }

    if (ErasureThreadPool_.HasValue()) {
        ErasureThreadPool_->Shutdown();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

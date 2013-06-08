#include "config.h"
#include "dispatcher.h"

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

TDispatcher::TDispatcher()
    : CompressionPoolSize(4)
    , ErasurePoolSize(4)
    , ReaderThread(TActionQueue::CreateFactory("ChunkReader"))
    , WriterThread(TActionQueue::CreateFactory("ChunkWriter"))
    , CompressionThreadPool(BIND(
        NYT::New<NYT::TThreadPool, const int&, const Stroka&>,
        ConstRef(CompressionPoolSize),
        "Compression"))
    , ErasureThreadPool(BIND(
        NYT::New<NYT::TThreadPool, const int&, const Stroka&>,
        ConstRef(ErasurePoolSize),
        "Erasure"))
{ }

TDispatcher* TDispatcher::Get()
{
    return Singleton<TDispatcher>();
}

void TDispatcher::Configure(TDispatcherConfigPtr config)
{
    // We believe in proper memory ordering here.
    YCHECK(!CompressionThreadPool.TryGet());
    // We do not really want to store entire config within us.
    CompressionPoolSize = config->CompressionPoolSize;
    // This is not redundant, since the check and the assignment above are
    // not atomic and (adversary) thread can initialize thread pool in parallel.
    YCHECK(!CompressionThreadPool.TryGet());

    // We believe in proper memory ordering here.
    YCHECK(!ErasureThreadPool.TryGet());
    // We do not really want to store entire config within us.
    ErasurePoolSize = config->ErasurePoolSize;
    // This is not redundant, since the check and the assignment above are
    // not atomic and (adversary) thread can initialize thread pool in parallel.
    YCHECK(!ErasureThreadPool.TryGet());
}

IInvokerPtr TDispatcher::GetReaderInvoker()
{
    return ReaderThread->GetInvoker();
}

IInvokerPtr TDispatcher::GetWriterInvoker()
{
    return WriterThread->GetInvoker();
}

IInvokerPtr TDispatcher::GetCompressionInvoker()
{
    return CompressionThreadPool->GetInvoker();
}

IInvokerPtr TDispatcher::GetErasureInvoker()
{
    return ErasureThreadPool->GetInvoker();
}

void TDispatcher::Shutdown()
{
    ReaderThread->Shutdown();
    WriterThread->Shutdown();
    CompressionThreadPool->Shutdown();
    ErasureThreadPool->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

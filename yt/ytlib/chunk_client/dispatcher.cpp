#include "config.h"
#include "dispatcher.h"

namespace NYT {
namespace NChunkClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TDispatcher::TDispatcher()
    : CompressionPoolSize(4)
    , ErasurePoolSize(4)
    , ReaderThread(
        TActionQueue::CreateFactory("ChunkReader"))
    , WriterThread(
        TActionQueue::CreateFactory("ChunkWriter"))
    , CompressionThreadPool(BIND(
        NYT::New<TThreadPool, const int&, const Stroka&>,
        ConstRef(CompressionPoolSize),
        "Compression"))
    , ErasureThreadPool(BIND(
        NYT::New<TThreadPool, const int&, const Stroka&>,
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
    YCHECK(!CompressionThreadPool);
    // We do not really want to store entire config within us.
    CompressionPoolSize = config->CompressionPoolSize;
    // This is not redundant, since the check and the assignment above are
    // not atomic and (adversary) thread can initialize thread pool in parallel.
    YCHECK(!CompressionThreadPool);

    // We believe in proper memory ordering here.
    YCHECK(!ErasureThreadPool);
    // We do not really want to store entire config within us.
    ErasurePoolSize = config->ErasurePoolSize;
    // This is not redundant, since the check and the assignment above are
    // not atomic and (adversary) thread can initialize thread pool in parallel.
    YCHECK(!ErasureThreadPool);
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
    if (ReaderThread) {
        ReaderThread->Shutdown();
    }

    if (WriterThread) {
        WriterThread->Shutdown();
    }

    if (CompressionThreadPool) {
        CompressionThreadPool->Shutdown();
    }

    if (ErasureThreadPool) {
        ErasureThreadPool->Shutdown();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

#include "dispatcher.h"

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

TDispatcher::TDispatcher()
    : PoolSize(4)
    , ReaderThread(TActionQueue::CreateFactory("ChunkReader"))
    , WriterThread(TActionQueue::CreateFactory("ChunkWriter"))
    , CompressionThreadPool(BIND(
        NYT::New<NYT::TThreadPool, const int&, const Stroka&>,
        ConstRef(PoolSize),
        "Compression"))
{ }

TDispatcher* TDispatcher::Get()
{
    return Singleton<TDispatcher>();
}

void TDispatcher::SetPoolSize(int poolSize)
{
    // We believe in proper memory ordering here.
    YCHECK(!CompressionThreadPool.TryGet());
    PoolSize = poolSize;
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

void TDispatcher::Shutdown()
{
    ReaderThread->Shutdown();
    WriterThread->Shutdown();
    CompressionThreadPool->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT


#pragma once

#include <ytlib/misc/lazy_ptr.h>

#include <ytlib/actions/action_queue.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TDispatcher
{
public:
    TDispatcher();

    static TDispatcher* Get();

    void SetPoolSize(int poolSize);

    IInvokerPtr GetReaderInvoker();
    IInvokerPtr GetWriterInvoker();
    IInvokerPtr GetCompressionInvoker();

    void Shutdown();

private:
    int PoolSize;

    /*!
     * This thread is used for background operations in #TRemoteChunkReader
     * #TSequentialChunkReader, #TTableChunkReader and #TableReader
     */
    TLazyPtr<TActionQueue> ReaderThread;
    /*!
     *  This thread is used for background operations in 
     *  #TRemoteChunkWriter, #NTableClient::TChunkWriter and 
     *  #NTableClient::TChunkSetReader
     */
    TLazyPtr<TActionQueue> WriterThread;

    TLazyPtr<TThreadPool> CompressionThreadPool;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT


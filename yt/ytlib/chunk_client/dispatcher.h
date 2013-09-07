#pragma once

#include "public.h"

#include <ytlib/misc/lazy_ptr.h>

#include <ytlib/concurrency/action_queue.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TDispatcher
{
public:
    TDispatcher();

    static TDispatcher* Get();

    void Configure(TDispatcherConfigPtr config);

    IInvokerPtr GetReaderInvoker();
    IInvokerPtr GetWriterInvoker();
    IInvokerPtr GetCompressionInvoker();
    IInvokerPtr GetErasureInvoker();

    void Shutdown();

private:
    int CompressionPoolSize;
    int ErasurePoolSize;

    /*!
     * This thread is used for background operations in #TRemoteChunkReader
     * #TSequentialChunkReader, #TTableChunkReader and #TableReader
     */
    TLazyIntrusivePtr<TActionQueue> ReaderThread;
    /*!
     *  This thread is used for background operations in
     *  #TRemoteChunkWriter, #NTableClient::TChunkWriter and
     *  #NTableClient::TChunkSetReader
     */
    TLazyIntrusivePtr<TActionQueue> WriterThread;

    TLazyIntrusivePtr<TThreadPool> CompressionThreadPool;

    TLazyIntrusivePtr<TThreadPool> ErasureThreadPool;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT


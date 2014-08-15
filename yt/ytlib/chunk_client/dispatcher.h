#pragma once

#include "public.h"

#include <core/misc/lazy_ptr.h>

#include <core/concurrency/action_queue.h>

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
    int CompressionPoolSize_;
    int ErasurePoolSize_;

    /*!
     * This thread is used for background operations in #TRemoteChunkReader
     * #TSequentialChunkReader, #TTableChunkReader and #TableReader
     */
    TLazyIntrusivePtr<NConcurrency::TActionQueue> ReaderThread_;

    /*!
     *  This thread is used for background operations in
     *  #TRemoteChunkWriter, #NTableClient::TChunkWriter and
     *  #NTableClient::TChunkSetReader
     */
    TLazyIntrusivePtr<NConcurrency::TActionQueue> WriterThread_;

    TLazyIntrusivePtr<NConcurrency::TThreadPool> CompressionThreadPool_;

    TLazyIntrusivePtr<NConcurrency::TThreadPool> ErasureThreadPool_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT


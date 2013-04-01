#pragma once

#include "public.h"
#include "config.h"
#include "remote_writer.h"

//#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/async_stream_state.h>

#include <ytlib/actions/parallel_awaiter.h>

//#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/table_client/table_reader.pb.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/master_ypath_proxy.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/logging/tagged_logger.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkWriter>
class TMultiChunkSequentialWriter
    : virtual public TRefCounted
{
public:
    typedef typename TChunkWriter::TProvider TProvider;
    typedef TIntrusivePtr<TProvider> TProviderPtr;

    typedef typename TChunkWriter::TFacade TFacade;

    TMultiChunkSequentialWriter(
        TMultiChunkWriterConfigPtr config,
        TMultiChunkWriterOptionsPtr options,
        TProviderPtr provider,
        NRpc::IChannelPtr masterChannel,
        const NTransactionClient::TTransactionId& transactionId,
        const NChunkClient::TChunkListId& parentChunkListId);

    ~TMultiChunkSequentialWriter();

    TAsyncError AsyncOpen();
    TAsyncError AsyncClose();

    // Returns pointer to writer facade, which allows single write operation.
    // In nullptr is returned, caller should subscribe to ReadyEvent.
    TFacade* GetCurrentWriter();
    TAsyncError GetReadyEvent();

    void SetProgress(double progress);

    /*!
     *  To get consistent data, should be called only when the writer is closed.
     */
    const std::vector<NTableClient::NProto::TInputChunk>& GetWrittenChunks() const;
    TProviderPtr GetProvider();

protected:
    struct TSession
    {
        TIntrusivePtr<TChunkWriter> ChunkWriter;
        NChunkClient::TRemoteWriterPtr RemoteWriter;

        TSession()
            : ChunkWriter(NULL)
            , RemoteWriter(NULL)
        { }

        bool IsNull() const
        {
            return !ChunkWriter;
        }

        void Reset()
        {
            ChunkWriter.Reset();
            RemoteWriter.Reset();
        }
    };

    void CreateNextSession();
    virtual void InitCurrentSession(TSession nextSession);

    void OnChunkCreated(NObjectClient::TMasterYPathProxy::TRspCreateObjectPtr rsp);

    void FinishCurrentSession();

    void OnChunkClosed(
        int chunkIndex,
        TSession currentSession,
        TAsyncErrorPromise finishResult,
        TError error);

    void OnChunkConfirmed(
        NChunkClient::TChunkId chunkId,
        TAsyncErrorPromise finishResult,
        NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);

    void OnChunkFinished(
        NChunkClient::TChunkId chunkId,
        TError error);

    void OnRowWritten();

    void AttachChunks();
    void OnClose(NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);

    void SwitchSession();

    const TMultiChunkWriterConfigPtr Config;
    const TMultiChunkWriterOptionsPtr Options;
    const NRpc::IChannelPtr MasterChannel;
    const NTransactionClient::TTransactionId TransactionId;
    const NChunkClient::TChunkListId ParentChunkListId;
    const int UploadReplicationFactor;

    TProviderPtr Provider;

    volatile double Progress;

    //! Total compressed size of data in the completed chunks.
    i64 CompleteChunkSize;

    TAsyncStreamState State;

    TSession CurrentSession;
    TPromise<TSession> NextSession;

    TParallelAwaiterPtr CloseChunksAwaiter;

    TSpinLock WrittenChunksGuard;
    std::vector<NTableClient::NProto::TInputChunk> WrittenChunks;

    NLog::TTaggedLogger Logger;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

#define MULTI_CHUNK_SEQUENTIAL_WRITER_INL_H_
#include "multi_chunk_sequential_writer-inl.h"
#undef MULTI_CHUNK_SEQUENTIAL_WRITER_INL_H_


#pragma once

#include "public.h"
#include "config.h"

#include <ytlib/table_client/table_reader.pb.h>
#include <ytlib/actions/parallel_awaiter.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/async_stream_state.h>
#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/remote_writer.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/chunk_server/chunk_service_proxy.h>
#include <ytlib/object_server/object_service_proxy.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/transaction_server/transaction_ypath_proxy.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkWriter>
class TChunkSequenceWriterBase
    : virtual public TRefCounted
{
public:
    TChunkSequenceWriterBase(
        TChunkSequenceWriterConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        const NTransactionClient::TTransactionId& transactionId,
        const NChunkServer::TChunkListId& parentChunkList);

    ~TChunkSequenceWriterBase();

    TAsyncError AsyncOpen();
    TAsyncError AsyncClose();

    void SetProgress(double progress);

    /*! 
     *  To get consistent data, should be called only when the writer is closed.
     */
    const std::vector<NProto::TInputChunk>& GetWrittenChunks() const;

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

    void OnChunkCreated(NTransactionServer::TTransactionYPathProxy::TRspCreateObject::TPtr rsp);
    virtual void PrepareChunkWriter(TSession& newSession) = 0;

    void FinishCurrentSession();

    void OnChunkClosed(
        TSession currentSession,
        TAsyncErrorPromise finishResult,
        TError error);

    void OnChunkRegistered(
        NChunkServer::TChunkId chunkId,
        TAsyncErrorPromise finishResult,
        NObjectServer::TObjectServiceProxy::TRspExecuteBatch::TPtr batchRsp);

    void OnChunkFinished(
        NChunkServer::TChunkId chunkId,
        TError error);

    void OnRowWritten(TError error);

    void OnClose();

    void SwitchSession();

    TChunkSequenceWriterConfigPtr Config;
    NRpc::IChannelPtr MasterChannel;

    volatile double Progress;

    //! Total compressed size of data in the completed chunks.
    i64 CompleteChunkSize;

    const NObjectServer::TTransactionId TransactionId;
    const NChunkServer::TChunkListId ParentChunkList;

    TAsyncStreamState State;

    TSession CurrentSession;
    TPromise<TSession> NextSession;

    TParallelAwaiter::TPtr CloseChunksAwaiter;

    TSpinLock WrittenChunksGuard;
    std::vector<NProto::TInputChunk> WrittenChunks;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);

private:
    NLog::TLogger& Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

#define CHUNK_SEQUENCE_WRITER_BASE_INL_H_
#include "chunk_sequence_writer_base-inl.h"
#undef CHUNK_SEQUENCE_WRITER_BASE_INL_H_


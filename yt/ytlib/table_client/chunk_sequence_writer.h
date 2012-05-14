#pragma once

#include "public.h"
#include "schema.h"
#include "async_writer.h"
#include "chunk_writer.h"

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

class TChunkSequenceWriter
    : public IAsyncWriter
{
public:
    TChunkSequenceWriter(
        TChunkSequenceWriterConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        const NTransactionClient::TTransactionId& transactionId,
        const NChunkServer::TChunkListId& parentChunkList,
        const std::vector<TChannel>& channels,
        const TNullable<TKeyColumns>& keyColumns = Null);

    ~TChunkSequenceWriter();

    TAsyncError AsyncOpen();

    TAsyncError AsyncWriteRow(TRow& row, const TNonOwningKey& key);
    TAsyncError AsyncClose();

    void SetProgress(double progress);

    const TOwningKey& GetLastKey() const;

    const TNullable<TKeyColumns>& GetKeyColumns() const;

    //! Current row count.
    i64 GetRowCount() const;

    /*! 
     *  To get consistent data, should be called only when the writer is closed.
     */
    const std::vector<NProto::TInputChunk>& GetWrittenChunks() const;

private:
    // Tools for writing single chunk.
    struct TSession
    {
        TChunkWriterPtr ChunkWriter;
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
    void InitCurrentSession(TSession nextSession);
    void OnChunkCreated(NTransactionServer::TTransactionYPathProxy::TRspCreateObject::TPtr rsp);

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
    const std::vector<TChannel> Channels;
    const TNullable<TKeyColumns> KeyColumns;

    volatile double Progress;

    //! Total compressed size of data in the completed chunks.
    i64 CompleteChunkSize;
    i64 RowCount;

    const NObjectServer::TTransactionId TransactionId;
    const NChunkServer::TChunkListId ParentChunkList;

    TAsyncStreamState State;

    TSession CurrentSession;
    TPromise<TSession> NextSession;

    TParallelAwaiter::TPtr CloseChunksAwaiter;

    TSpinLock WrittenChunksGuard;
    std::vector<NProto::TInputChunk> WrittenChunks;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

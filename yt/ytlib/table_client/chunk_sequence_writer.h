#pragma once

#include "public.h"
#include "schema.h"
#include "async_writer.h"
#include "chunk_writer.h"

#include <ytlib/actions/parallel_awaiter.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/async_stream_state.h>
#include <ytlib/chunk_client/public.h>
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

    TAsyncError AsyncWriteRow(TRow& row, TKey& key);
    TAsyncError AsyncClose();

    void SetProgress(double progress);

    TKey& GetLastKey();

    const TNullable<TKeyColumns>& GetKeyColumns() const;

    //! Current row count.
    i64 GetRowCount() const;

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
            return !bool(ChunkWriter);
        }

        void Reset()
        {
            ChunkWriter.Reset();
            RemoteWriter.Reset();
        }
    };

    void CreateNextSession();
    void InitCurrentSession(TSession nextSession);
    void OnSessionCreated(
        NTransactionServer::TTransactionYPathProxy::TRspCreateObject::TPtr rsp);

    void FinishCurrentChunk();

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

    void OnRowEnded(TError error);

    void OnClose();

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

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

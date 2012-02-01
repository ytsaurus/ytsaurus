#pragma once
#include "common.h"
#include "schema.h"
#include "writer.h"
#include "chunk_writer.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/actions/parallel_awaiter.h>
#include <ytlib/chunk_server/chunk_service_proxy.h>
#include <ytlib/chunk_server/id.h>
#include <ytlib/chunk_client/remote_writer.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/cypress/cypress_service_proxy.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkSequenceWriter
    : public IWriter
{
public:
    typedef TIntrusivePtr<TChunkSequenceWriter> TPtr;

    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        i64 MaxChunkSize;

        //! When current chunk size relative to #MaxChunkSize overcomes this threshold (given in percents)
        //! the writer prepares the next chunk.
        int NextChunkThreshold;

        int TotalReplicaCount;
        int UploadReplicaCount;

        TChunkWriter::TConfig::TPtr ChunkWriter;
        NChunkClient::TRemoteWriter::TConfig::TPtr RemoteWriter;

        TConfig()
        {
            Register("max_chunk_size", MaxChunkSize)
                .GreaterThan(0)
                .Default(1024 * 1024 * 1024);
            Register("next_chunk_threshold", NextChunkThreshold)
                .GreaterThan(0)
                .LessThan(100)
                .Default(70);
            Register("total_replica_count", TotalReplicaCount)
                .GreaterThanOrEqual(1)
                .Default(3);
            Register("upload_replica_count", UploadReplicaCount)
                .GreaterThanOrEqual(1)
                .Default(2);
            Register("chunk_writer", ChunkWriter)
                .DefaultNew();
            Register("remote_writer", RemoteWriter)
                .DefaultNew();
        }

        virtual void DoValidate() const
        {
            if (TotalReplicaCount < UploadReplicaCount) {
                ythrow yexception() << "\"total_replica_count\" cannot be less than \"upload_replica_count\"";
            }
        }
    };

    TChunkSequenceWriter(
        TConfig* config,
        NRpc::IChannel* masterChannel,
        const NTransactionClient::TTransactionId& transactionId,
        const NChunkServer::TChunkListId& parentChunkList,
        const TSchema& schema);

    ~TChunkSequenceWriter();

    TAsyncError::TPtr AsyncOpen();
    void Write(const TColumn& column, TValue value);
    TAsyncError::TPtr AsyncEndRow();
    TAsyncError::TPtr AsyncClose();
    void Cancel(const TError& error);

private:
    typedef NChunkServer::TChunkServiceProxy TProxy;

    void CreateNextChunk();
    void InitCurrentChunk(TChunkWriter::TPtr nextChunk);
    void OnChunkCreated(TProxy::TRspCreateChunks::TPtr rsp);

    bool IsNextChunkTime() const;

    void FinishCurrentChunk();
    void OnChunkClosed(
        TError error,
        TChunkWriter::TPtr currentChunk,
        TAsyncError::TPtr finishResult);
    void OnChunkRegistered(
        NCypress::TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp,
        NChunkClient::TChunkId chunkId,
        TAsyncError::TPtr finishResult);
    void OnChunkFinished(
        TError error,
        NChunkClient::TChunkId chunkId);

    void OnChunkRegistered();

    void OnRowEnded(TError error);
    void OnClose();

    TConfig::TPtr Config;

    TProxy ChunkProxy;
    NCypress::TCypressServiceProxy CypressProxy;

    const NObjectServer::TTransactionId TransactionId;
    const NChunkServer::TChunkListId ParentChunkList;

    const TSchema Schema;

    TAsyncStreamState State;

    //! Protects #CurrentChunk.
    TSpinLock CurrentSpinLock;
    TChunkWriter::TPtr CurrentChunk;
    TFuture<TChunkWriter::TPtr>::TPtr NextChunk;

    TParallelAwaiter::TPtr CloseChunksAwaiter;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

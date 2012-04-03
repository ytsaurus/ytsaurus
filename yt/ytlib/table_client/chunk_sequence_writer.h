#pragma once
#include "common.h"
#include "schema.h"
#include "async_writer.h"
#include "chunk_writer.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/actions/parallel_awaiter.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/chunk_server/chunk_service_proxy.h>
#include <ytlib/chunk_client/remote_writer.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/cypress/cypress_service_proxy.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkSequenceWriter
    : public IAsyncBlockWriter
{
public:
    typedef TIntrusivePtr<TChunkSequenceWriter> TPtr;

    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        i64 MaxChunkSize;

        //! When current chunk size relative to #MaxChunkSize overcomes this threshold (given as a fraction)
        //! the writer prepares the next chunk.
        double NextChunkThreshold;

        int TotalReplicaCount;
        int UploadReplicaCount;

        TChunkWriter::TConfig::TPtr ChunkWriter;
        NChunkClient::TRemoteWriter::TConfig::TPtr RemoteWriter;

        TConfig()
        {
            Register("max_chunk_size", MaxChunkSize)
                .GreaterThan(0)
                .Default(1024 * 1024 * 1024);
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
        const NChunkServer::TChunkListId& parentChunkList);

    ~TChunkSequenceWriter();

    TAsyncError AsyncOpen(
        const NProto::TTableChunkAttributes& attributes);

    TAsyncError AsyncEndRow(
        TKey& key,
        std::vector<TChannelWriter::TPtr>& channels);

    TAsyncError AsyncClose(
        TKey& lastKey,
        std::vector<TChannelWriter::TPtr>& channels);

private:
    typedef NChunkServer::TChunkServiceProxy TProxy;

    void CreateNextChunk();
    void InitCurrentChunk(TChunkWriter::TPtr nextChunk);
    void OnChunkCreated(TProxy::TRspCreateChunks::TPtr rsp);

    bool IsNextChunkTime() const;

    void FinishCurrentChunk(
        TKey& lastKey,
        std::vector<TChannelWriter::TPtr>& channels);

    void OnChunkClosed(
        TChunkWriter::TPtr currentChunk,
        TAsyncError finishResult,
        TError error);

    void OnChunkRegistered(
        NChunkClient::TChunkId chunkId,
        TAsyncError finishResult,
        NCypress::TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp);

    void OnChunkFinished(
        NChunkClient::TChunkId chunkId,
        TError error);

    void OnRowEnded(TError error);
    void OnClose();

    TConfig::TPtr Config;

    /*const i64 ExpectedRowCount;
    i64 CurrentRowCount;
    i64 */

    TProxy ChunkProxy;
    NCypress::TCypressServiceProxy CypressProxy;

    const NObjectServer::TTransactionId TransactionId;
    const NChunkServer::TChunkListId ParentChunkList;

    TAsyncStreamState State;

    //! Protects #CurrentChunk.
    TChunkWriter::TPtr CurrentChunk;
    TFuture<TChunkWriter::TPtr>::TPtr NextChunk;

    TParallelAwaiter::TPtr CloseChunksAwaiter;
    NProto::TTableChunkAttributes Attributes;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

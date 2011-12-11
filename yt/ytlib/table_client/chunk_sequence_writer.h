#pragma once
#include "common.h"
#include "schema.h"
#include "writer.h"
#include "chunk_writer.h"

#include "../misc/thread_affinity.h"

#include "../chunk_server/chunk_service_rpc.h"
#include "../chunk_client/remote_writer.h"
#include "../transaction_client/transaction.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkSequenceWriter
    : public IWriter
{
public:
    typedef TIntrusivePtr<TChunkSequenceWriter> TPtr;

    struct TConfig
        : public TConfigBase
    {
        i64 MaxChunkSize;

        //! When current chunk size relative to #MaxChunkSize
        //! overcomes this threshold (in percents), it's time to prepare the next chunk.
        int NextChunkThreshold;

        int ReplicationFactor;

        TChunkWriter::TConfig TableChunk;
        NChunkClient::TRemoteWriter::TConfig RemoteChunk;

        TConfig()
        {
            Register("max_chunk_size", MaxChunkSize).GreaterThan(0).Default(256 * 1024 * 1024);
            Register("next_chunk_threshold", NextChunkThreshold).GreaterThan(0).LessThan(100).Default(70);
            Register("replication_factor", ReplicationFactor).GreaterThanOrEqual(1).Default(2);
            Register("table_chunk", TableChunk);
            Register("remote_chunk", RemoteChunk);

            SetDefaults();
        }
    };

    TChunkSequenceWriter(
        const TConfig& config,
        const TSchema& schema,
        const NTransactionClient::TTransactionId& transactionId,
        NRpc::IChannel::TPtr masterChannel);

    TAsyncStreamState::TAsyncResult::TPtr AsyncOpen();
    void Write(const TColumn& column, TValue value);
    TAsyncStreamState::TAsyncResult::TPtr AsyncEndRow();
    TAsyncStreamState::TAsyncResult::TPtr AsyncClose();
    void Cancel(const Stroka& errorMessage);

    const yvector<NChunkClient::TChunkId>& GetWrittenChunks() const;

private:
    typedef NChunkServer::TChunkServiceProxy TProxy;

        void CreateNextChunk();
    void InitCurrentChunk(TChunkWriter::TPtr nextChunk);
    void FinishCurrentChunk();

    bool IsNextChunkTime() const;

    void OnChunkCreated(TProxy::TRspAllocateChunk::TPtr rsp);

    void OnChunkClosed(
        TAsyncStreamState::TResult result, 
        NChunkClient::TChunkId chunkId);

    void OnRowEnded(TAsyncStreamState::TResult result);
    void OnClose();

    const TConfig Config;
    const TSchema Schema;

    const NTransactionClient::TTransactionId TransactionId;

    TAsyncStreamState State;
    TProxy Proxy;

    //! Protects #CurrentChunk.
    TSpinLock CurrentSpinLock;
    TChunkWriter::TPtr CurrentChunk;
    TFuture<TChunkWriter::TPtr>::TPtr NextChunk;

    //! Protects #WrittenChunks.
    TSpinLock WrittenSpinLock;
    yvector<NChunkClient::TChunkId> WrittenChunks;

    TParallelAwaiter::TPtr CloseChunksAwaiter;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

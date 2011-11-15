#pragma once
#include "common.h"
#include "schema.h"
#include "writer.h"
#include "chunk_writer.h"

#include "../misc/thread_affinity.h"

#include "../chunk_server/chunk_service_rpc.h"
#include "../chunk_client/remote_chunk_writer.h"
#include "../transaction_client/transaction.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

// TODO: rename to TChunkSequenceWriter
class TChunkSetWriter
    : public IWriter
{
public:
    typedef TIntrusivePtr<TChunkSetWriter> TPtr;

    struct TConfig
    {
        i64 MaxChunkSize;

        //! When current chunk size relative to #MaxChunkSize
        //! overcomes this threshold, it's time to prepare the next chunk.
        double NextChunkThreshold;

        int ReplicationFactor;

        TChunkWriter::TConfig TableChunkConfig;
        TRemoteChunkWriter::TConfig ChunkWriterConfig;

        TConfig(
            i64 chunkSize, 
            double threshold,
            int repFactor, 
            const TRemoteChunkWriter::TConfig& chunkWriterConfig)
            : MaxChunkSize(chunkSize)
            , NextChunkThreshold(threshold)
            , ReplicationFactor(repFactor)
            , ChunkWriterConfig(chunkWriterConfig)
        { }
    };

    TChunkSetWriter(
        const TConfig& config,
        const TSchema& schema,
        ICodec* codec,
        const NTransactionClient::TTransactionId& transactionId,
        NRpc::IChannel::TPtr masterChannel);

    // TODO: -> Open
    TAsyncStreamState::TAsyncResult::TPtr AsyncInit();
    void Write(const TColumn& column, TValue value);
    TAsyncStreamState::TAsyncResult::TPtr AsyncEndRow();
    TAsyncStreamState::TAsyncResult::TPtr AsyncClose();
    void Cancel(const Stroka& errorMessage);

    const yvector<TChunkId>& GetWrittenChunks() const;

private:
    TAsyncStreamState State;

    TSchema Schema;
    ICodec* Codec;

    typedef NChunkServer::TChunkServiceProxy TProxy;
    USE_RPC_PROXY_METHOD(TProxy, CreateChunk)

    void CreateNextChunk();
    void InitCurrentChunk(TChunkWriter::TPtr nextChunk);
    void FinishCurrentChunk();

    bool IsNextChunkTime() const;

    void OnChunkCreated(TRspCreateChunk::TPtr rsp);

    void OnChunkClosed(
        TAsyncStreamState::TResult result, 
        TChunkId chunkId);

    void OnRowEnded(TAsyncStreamState::TResult result);
    void OnClose();

    TConfig Config;
    NTransactionClient::TTransactionId TransactionId;

    //! Protects #CurrentChunk.
    TSpinLock CurrentSpinLock;
    TChunkWriter::TPtr CurrentChunk;
    TFuture<TChunkWriter::TPtr>::TPtr NextChunk;

    //! Protects #WrittenChunks.
    TSpinLock WrittenSpinLock;
    yvector<TChunkId> WrittenChunks;

    TParallelAwaiter::TPtr CloseChunksAwaiter;

    TProxy Proxy;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
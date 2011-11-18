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

using namespace NYT::NChunkClient;

////////////////////////////////////////////////////////////////////////////////

// TODO: rename to TChunkSequenceWriter
class TChunkSequenceWriter
    : public IWriter
{
public:
    typedef TIntrusivePtr<TChunkSequenceWriter> TPtr;

    struct TConfig
    {
        i64 MaxChunkSize;

        //! When current chunk size relative to #MaxChunkSize
        //! overcomes this threshold, it's time to prepare the next chunk.
        double NextChunkThreshold;

        int ReplicationFactor;

        TChunkWriter::TConfig TableChunkConfig;
        TRemoteWriter::TConfig ChunkWriterConfig;

        TConfig(
            i64 chunkSize, 
            double threshold,
            int repFactor, 
            const TRemoteWriter::TConfig& chunkWriterConfig)
            : MaxChunkSize(chunkSize)
            , NextChunkThreshold(threshold)
            , ReplicationFactor(repFactor)
            , ChunkWriterConfig(chunkWriterConfig)
        { }
    };

    TChunkSequenceWriter(
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

    const TConfig Config;
    const TSchema Schema;
    ICodec* Codec;
    const NTransactionClient::TTransactionId TransactionId;

    TAsyncStreamState State;
    TProxy Proxy;

    //! Protects #CurrentChunk.
    TSpinLock CurrentSpinLock;
    TChunkWriter::TPtr CurrentChunk;
    TFuture<TChunkWriter::TPtr>::TPtr NextChunk;

    //! Protects #WrittenChunks.
    TSpinLock WrittenSpinLock;
    yvector<TChunkId> WrittenChunks;

    TParallelAwaiter::TPtr CloseChunksAwaiter;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
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
    {
        i64 MaxChunkSize;

        //! When current chunk size relative to #MaxChunkSize
        //! overcomes this threshold, it's time to prepare the next chunk.
        double NextChunkThreshold;

        int ReplicationFactor;

        TChunkWriter::TConfig TableChunkConfig;
        NChunkClient::TRemoteWriter::TConfig ChunkWriterConfig;

        TConfig(
            i64 chunkSize, 
            double threshold,
            int repFactor, 
            const NChunkClient::TRemoteWriter::TConfig& chunkWriterConfig)
            : MaxChunkSize(chunkSize)
            , NextChunkThreshold(threshold)
            , ReplicationFactor(repFactor)
            , ChunkWriterConfig(chunkWriterConfig)
        { }
    };

    TChunkSequenceWriter(
        const TConfig& config,
        const TSchema& schema,
        const NTransactionClient::TTransactionId& transactionId,
        NRpc::IChannel::TPtr masterChannel);

    TAsyncError::TPtr AsyncOpen();
    void Write(const TColumn& column, TValue value);
    TAsyncError::TPtr AsyncEndRow();
    TAsyncError::TPtr AsyncClose();
    void Cancel(const TError& error);

    const yvector<NChunkClient::TChunkId>& GetWrittenChunks() const;

private:
    typedef NChunkServer::TChunkServiceProxy TProxy;

        void CreateNextChunk();
    void InitCurrentChunk(TChunkWriter::TPtr nextChunk);
    void FinishCurrentChunk();

    bool IsNextChunkTime() const;

    void OnChunkCreated(TProxy::TRspAllocateChunk::TPtr rsp);

    void OnChunkClosed(
        TError error, 
        NChunkClient::TChunkId chunkId);

    void OnRowEnded(TError error);
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

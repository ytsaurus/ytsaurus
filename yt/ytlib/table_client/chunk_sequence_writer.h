#pragma once
#include "common.h"
#include "schema.h"
#include "writer.h"
#include "chunk_writer.h"

#include "../misc/thread_affinity.h"

#include "../chunk_server/chunk_service_proxy.h"
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
            Register("max_chunk_size", MaxChunkSize).GreaterThan(0).Default(256 * 1024 * 1024);
            Register("next_chunk_threshold", NextChunkThreshold).GreaterThan(0).LessThan(100).Default(70);
            Register("total_replica_count", UploadReplicaCount).GreaterThanOrEqual(1).Default(3);
            Register("upload_replica_count", UploadReplicaCount).GreaterThanOrEqual(1).Default(2);
            Register("chunk_writer", ChunkWriter).DefaultNew();
            Register("remote_writer", RemoteWriter).DefaultNew();
        }

        virtual void Validate(const NYTree::TYPath& path = "/")
        {
            TConfigurable::Validate(path);

            if (TotalReplicaCount < UploadReplicaCount) {
                ythrow yexception() << "\"total_replica_count\" cannot be less than \"upload_replica_count\"";
            }
        }
    };

    TChunkSequenceWriter(
        TConfig* config,
        NRpc::IChannel* masterChannel,
        const NTransactionClient::TTransactionId& transactionId,
        const TSchema& schema);

    TAsyncError::TPtr AsyncOpen();
    void Write(const TColumn& column, TValue value);
    TAsyncError::TPtr AsyncEndRow();
    TAsyncError::TPtr AsyncClose();
    void Cancel(const TError& error);

    const yvector<NChunkClient::TChunkId>& GetWrittenChunkIds() const;

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

    TConfig::TPtr Config;
    TProxy Proxy;
    const NTransactionClient::TTransactionId TransactionId;
    const TSchema Schema;

    TAsyncStreamState State;

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

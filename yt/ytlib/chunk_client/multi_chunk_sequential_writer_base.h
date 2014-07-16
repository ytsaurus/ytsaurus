#pragma once

#include "public.h"

#include "chunk_replica.h"
#include "data_statistics.h"
#include "multi_chunk_writer.h"

#include <ytlib/node_tracker_client/public.h>

#include <ytlib/transaction_client/public.h>

#include <core/concurrency/parallel_awaiter.h>

#include <core/rpc/public.h>

#include <core/logging/log.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkSequentialWriterBase
    : public virtual IMultiChunkWriter
{
public:
    TMultiChunkSequentialWriterBase(
        TMultiChunkWriterConfigPtr config,
        TMultiChunkWriterOptionsPtr options,
        NRpc::IChannelPtr masterChannel,
        const NTransactionClient::TTransactionId& transactionId,
        const TChunkListId& parentChunkListId);

    virtual TAsyncError Open() override;
    virtual TAsyncError Close() override;

    virtual TAsyncError GetReadyEvent() override;

    void SetProgress(double progress);

    /*!
     *  To get consistent data, should be called only when the writer is closed.
     */
    const std::vector<NProto::TChunkSpec>& GetWrittenChunks() const;

    //! Provides node id to descriptor mapping for chunks returned via #GetWrittenChunks.
    NNodeTrackerClient::TNodeDirectoryPtr GetNodeDirectory() const;

    NProto::TDataStatistics GetDataStatistics() const;

protected:
    struct TSession
    {
        IChunkWriterBasePtr FrontalWriter;
        IWriterPtr UnderlyingWriter;
        std::vector<TChunkReplica> Replicas;
        TChunkId ChunkId;

        TSession()
            : FrontalWriter(nullptr)
            , UnderlyingWriter(nullptr)
        { }

        bool IsActive() const
        {
            return bool(FrontalWriter);
        }

        void Reset()
        {
            FrontalWriter = nullptr;
            UnderlyingWriter = nullptr;
            ChunkId = TChunkId();
            Replicas.clear();
        }
    };

    TMultiChunkWriterConfigPtr Config_;
    TMultiChunkWriterOptionsPtr Options_;
    NRpc::IChannelPtr MasterChannel_;
    NTransactionClient::TTransactionId TransactionId_;
    TChunkListId ParentChunkListId_;

    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;

    const int UploadReplicationFactor_;

    volatile double Progress_;

    TSession CurrentSession_;
    TSession NextSession_;

    bool Closing_;

    TFuture<void> NextSessionReady_;
    TAsyncError ReadyEvent_;

    TAsyncErrorPromise CompletionError_;

    NConcurrency::TParallelAwaiterPtr CloseChunksAwaiter_;

    NProto::TDataStatistics DataStatistics_;
    std::vector<NChunkClient::NProto::TChunkSpec> WrittenChunks_;

    NLog::TLogger Logger;


    TError DoOpen();
    void DoClose();

    void CreateNextSession();
    TError InitCurrentSession();

    bool VerifyActive();

    bool TrySwitchSession();
    void SwitchSession();
    TError DoSwitchSession(const TSession& session);

    TFuture<void> FinishSession(const TSession& session);
    void DoFinishSession(const TSession& session);

    virtual IChunkWriterBasePtr CreateFrontalWriter(IWriterPtr underlyingWriter) = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

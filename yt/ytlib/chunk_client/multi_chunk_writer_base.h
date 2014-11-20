#pragma once

#include "public.h"

#include "chunk_replica.h"
#include "data_statistics.h"
#include "multi_chunk_writer.h"

#include <ytlib/node_tracker_client/public.h>

#include <ytlib/transaction_client/public.h>

#include <core/concurrency/parallel_awaiter.h>

#include <core/logging/log.h>

#include <core/rpc/public.h>


namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TNontemplateMultiChunkWriterBase
    : public virtual IMultiChunkWriter
{
public:
    TNontemplateMultiChunkWriterBase(
        TMultiChunkWriterConfigPtr config,
        TMultiChunkWriterOptionsPtr options,
        NRpc::IChannelPtr masterChannel,
        const NTransactionClient::TTransactionId& transactionId,
        const TChunkListId& parentChunkListId);

    virtual TFuture<void> Open() override;
    virtual TFuture<void> Close() override;

    virtual TFuture<void> GetReadyEvent() override;

    void SetProgress(double progress);

    /*!
     *  To get consistent data, should be called only when the writer is closed.
     */
    const std::vector<NProto::TChunkSpec>& GetWrittenChunks() const;

    //! Provides node id to descriptor mapping for chunks returned via #GetWrittenChunks.
    NNodeTrackerClient::TNodeDirectoryPtr GetNodeDirectory() const;

    NProto::TDataStatistics GetDataStatistics() const;

protected:
    NLog::TLogger Logger;

    bool VerifyActive();
    bool TrySwitchSession();

    virtual IChunkWriterBasePtr CreateTemplateWriter(IChunkWriterPtr underlyingWriter) = 0;

private:
    struct TSession
    {
        IChunkWriterBasePtr TemplateWriter;
        IChunkWriterPtr UnderlyingWriter;
        std::vector<TChunkReplica> Replicas;
        TChunkId ChunkId;

        TSession()
            : TemplateWriter(nullptr)
            , UnderlyingWriter(nullptr)
        { }

        bool IsActive() const
        {
            return bool(TemplateWriter);
        }

        void Reset()
        {
            TemplateWriter = nullptr;
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
    TFuture<void> ReadyEvent_;

    TPromise<void> CompletionError_;

    NConcurrency::TParallelAwaiterPtr CloseChunksAwaiter_;

    NProto::TDataStatistics DataStatistics_;
    std::vector<NChunkClient::NProto::TChunkSpec> WrittenChunks_;


    void DoOpen();
    void DoClose();

    void CreateNextSession();
    void InitCurrentSession();

    void SwitchSession();
    void DoSwitchSession(const TSession& session);

    TFuture<void> FinishSession(const TSession& session);
    void DoFinishSession(const TSession& session);

};

////////////////////////////////////////////////////////////////////////////////

template <class IMultiChunkWriter, class ISpecificChunkWriter, class... TWriteArgs>
class TMultiChunkWriterBase
    : public TNontemplateMultiChunkWriterBase
    , public IMultiChunkWriter
{
public:
    typedef TIntrusivePtr<ISpecificChunkWriter> ISpecificChunkWriterPtr;

    TMultiChunkWriterBase(
        TMultiChunkWriterConfigPtr config,
        TMultiChunkWriterOptionsPtr options,
        NRpc::IChannelPtr masterChannel,
        const NTransactionClient::TTransactionId& transactionId,
        const TChunkListId& parentChunkListId,
        std::function<ISpecificChunkWriterPtr(IChunkWriterPtr)> createChunkWriter)
        : TNontemplateMultiChunkWriterBase(
            config, 
            options, 
            masterChannel, 
            transactionId, 
            parentChunkListId)
        , CreateChunkWriter_(createChunkWriter)
    { }

    virtual bool Write(TWriteArgs... args) override {
        if (!VerifyActive()) {
            return false;
        }

        // Return true if current writer is ready for more data and
        // we didn't switch to the next chunk.
        bool readyForMore = CurrentWriter_->Write(std::forward<TWriteArgs>(args)...);
        bool switched = false;
        if (readyForMore) {
            switched = TrySwitchSession();
        }
        return readyForMore && !switched;
    }

protected:
    ISpecificChunkWriterPtr CurrentWriter_;
    std::function<ISpecificChunkWriterPtr(IChunkWriterPtr)> CreateChunkWriter_;

    virtual IChunkWriterBasePtr CreateTemplateWriter(IChunkWriterPtr underlyingWriter) override
    {
        CurrentWriter_ = CreateChunkWriter_(underlyingWriter);
        return CurrentWriter_;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

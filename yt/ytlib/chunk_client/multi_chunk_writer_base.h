#pragma once

#include "multi_chunk_writer.h"

#include <yt/client/chunk_client/data_statistics.h>

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/logging/log.h>

#include <yt/core/rpc/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TNontemplateMultiChunkWriterBase
    : public virtual IMultiChunkWriter
{
public:
    TNontemplateMultiChunkWriterBase(
        TMultiChunkWriterConfigPtr config,
        TMultiChunkWriterOptionsPtr options,
        NApi::NNative::IClientPtr client,
        NObjectClient::TCellTag cellTag,
        NTransactionClient::TTransactionId transactionId,
        const TChunkListId& parentChunkListId,
        TTrafficMeterPtr trafficMeter,
        NConcurrency::IThroughputThrottlerPtr throttler,
        IBlockCachePtr blockCache);

    void Init();

    virtual TFuture<void> Close() override;

    virtual TFuture<void> GetReadyEvent() override;

    /*!
     *  To get consistent data, should be called only when the writer is closed.
     */
    virtual const std::vector<NProto::TChunkSpec>& GetWrittenChunksMasterMeta() const override;
    virtual const std::vector<NProto::TChunkSpec>& GetWrittenChunksFullMeta() const override;

    //! Provides node id to descriptor mapping for chunks returned via #GetWrittenChunks.
    virtual NNodeTrackerClient::TNodeDirectoryPtr GetNodeDirectory() const override;

    virtual NProto::TDataStatistics GetDataStatistics() const override;

    virtual TCodecStatistics GetCompressionStatistics() const override;

protected:
    NLogging::TLogger Logger;
    const NApi::NNative::IClientPtr Client_;

    bool TrySwitchSession();

    std::atomic<bool> SwitchingSession_ = {true};

    virtual IChunkWriterBasePtr CreateTemplateWriter(IChunkWriterPtr underlyingWriter) = 0;

private:
    struct TSession
    {
        IChunkWriterBasePtr TemplateWriter;
        IChunkWriterPtr UnderlyingWriter;

        bool IsActive() const
        {
            return bool(TemplateWriter);
        }

        void Reset()
        {
            TemplateWriter.Reset();
            UnderlyingWriter.Reset();
        }
    };

    const TMultiChunkWriterConfigPtr Config_;
    const TMultiChunkWriterOptionsPtr Options_;
    const NObjectClient::TCellTag CellTag_;
    const NTransactionClient::TTransactionId TransactionId_;
    const TChunkListId ParentChunkListId_;
    const NConcurrency::IThroughputThrottlerPtr Throttler_;
    const IBlockCachePtr BlockCache_;
    const NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;
    const TTrafficMeterPtr TrafficMeter_;

    TSession CurrentSession_;

    bool Closing_ = false;

    TFuture<void> ReadyEvent_ = VoidFuture;

    TSpinLock SpinLock_;
    NProto::TDataStatistics DataStatistics_;
    TCodecStatistics CodecStatistics;
    std::vector<NChunkClient::NProto::TChunkSpec> WrittenChunks_;
    std::vector<NChunkClient::NProto::TChunkSpec> WrittenChunksFullMeta_;

    void InitSession();
    void FinishSession();

    void SwitchSession();
    void DoSwitchSession();
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
        NApi::NNative::IClientPtr client,
        NObjectClient::TCellTag cellTag,
        NTransactionClient::TTransactionId transactionId,
        const TChunkListId& parentChunkListId,
        std::function<ISpecificChunkWriterPtr(IChunkWriterPtr)> createChunkWriter,
        TTrafficMeterPtr trafficMeter,
        NConcurrency::IThroughputThrottlerPtr throttler,
        IBlockCachePtr blockCache)
        : TNontemplateMultiChunkWriterBase(
            config,
            options,
            client,
            cellTag,
            transactionId,
            parentChunkListId,
            trafficMeter,
            throttler,
            blockCache)
        , CreateChunkWriter_(createChunkWriter)
    { }

    virtual bool Write(TWriteArgs... args) override
    {
        YCHECK(!SwitchingSession_);

        // Return true if current writer is ready for more data and
        // we didn't switch to the next chunk.
        bool readyForMore = CurrentWriter_->Write(std::forward<TWriteArgs>(args)...);
        bool switched = TrySwitchSession();
        return readyForMore && !switched;
    }

protected:
    const std::function<ISpecificChunkWriterPtr(IChunkWriterPtr)> CreateChunkWriter_;

    ISpecificChunkWriterPtr CurrentWriter_;

    virtual IChunkWriterBasePtr CreateTemplateWriter(IChunkWriterPtr underlyingWriter) override
    {
        CurrentWriter_ = CreateChunkWriter_(underlyingWriter);
        return CurrentWriter_;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

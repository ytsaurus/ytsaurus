#pragma once

#include "multi_chunk_writer.h"

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/public.h>

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
        TString localHostName,
        NObjectClient::TCellTag cellTag,
        NTransactionClient::TTransactionId transactionId,
        TChunkListId parentChunkListId,
        TTrafficMeterPtr trafficMeter,
        NConcurrency::IThroughputThrottlerPtr throttler,
        IBlockCachePtr blockCache);

    void Init();

    TFuture<void> Close() override;

    TFuture<void> GetReadyEvent() override;

    /*!
     *  To get consistent data, should be called only when the writer is closed.
     */
    const std::vector<NProto::TChunkSpec>& GetWrittenChunkSpecs() const override;

    const TChunkWithReplicasList& GetWrittenChunkWithReplicasList() const override;

    NProto::TDataStatistics GetDataStatistics() const override;

    TCodecStatistics GetCompressionStatistics() const override;

protected:
    NLogging::TLogger Logger;
    const NApi::NNative::IClientPtr Client_;

    bool TrySwitchSession();

    std::atomic<bool> SwitchingSession_ = true;

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
    const TString LocalHostName_;
    const NTransactionClient::TTransactionId TransactionId_;
    const TChunkListId ParentChunkListId_;
    const NConcurrency::IThroughputThrottlerPtr Throttler_;
    const IBlockCachePtr BlockCache_;
    const TTrafficMeterPtr TrafficMeter_;

    TSession CurrentSession_;

    bool Closing_ = false;

    TFuture<void> ReadyEvent_ = VoidFuture;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    NProto::TDataStatistics DataStatistics_;
    TCodecStatistics CodecStatistics;

    std::vector<NChunkClient::NProto::TChunkSpec> WrittenChunkSpecs_;
    TChunkWithReplicasList WrittenChunkWithReplicasList_;


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
        TString localHostName,
        NObjectClient::TCellTag cellTag,
        NTransactionClient::TTransactionId transactionId,
        TChunkListId parentChunkListId,
        std::function<ISpecificChunkWriterPtr(IChunkWriterPtr)> createChunkWriter,
        TTrafficMeterPtr trafficMeter,
        NConcurrency::IThroughputThrottlerPtr throttler,
        IBlockCachePtr blockCache)
        : TNontemplateMultiChunkWriterBase(
            config,
            options,
            client,
            std::move(localHostName),
            cellTag,
            transactionId,
            parentChunkListId,
            trafficMeter,
            throttler,
            blockCache)
        , CreateChunkWriter_(createChunkWriter)
    { }

    bool Write(TWriteArgs... args) override
    {
        YT_VERIFY(!SwitchingSession_);

        // Return true if current writer is ready for more data and
        // we didn't switch to the next chunk.
        bool readyForMore = CurrentWriter_->Write(std::forward<TWriteArgs>(args)...);
        bool switched = TrySwitchSession();
        return readyForMore && !switched;
    }

protected:
    const std::function<ISpecificChunkWriterPtr(IChunkWriterPtr)> CreateChunkWriter_;

    ISpecificChunkWriterPtr CurrentWriter_;

    IChunkWriterBasePtr CreateTemplateWriter(IChunkWriterPtr underlyingWriter) override
    {
        CurrentWriter_ = CreateChunkWriter_(underlyingWriter);
        return CurrentWriter_;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

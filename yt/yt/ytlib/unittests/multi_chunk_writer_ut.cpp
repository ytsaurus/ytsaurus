#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/chunk_client/chunk_writer_base.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/memory_writer.h>
#include <yt/yt/ytlib/chunk_client/multi_chunk_writer_base.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <library/cpp/yt/misc/enum.h>

#include <semaphore>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

class TMockSink
    : public IChunkWriter
{
public:
    const TMemoryWriterPtr Sink = New<TMemoryWriter>(
        MakeRandomId(EObjectType::Chunk, TCellTag(1)));
    TFuture<void> OpenResult = OKFuture;
    TFuture<void> ReadyResult = OKFuture;
    TFuture<void> CloseResult = OKFuture;
    bool DemandClose = false;

    TFuture<void> Open() override
    {
        return OpenResult.Apply(BIND([this, this_ = MakeStrong(this)] {
            return Sink->Open();
        }));
    }

    bool WriteBlock(
        const TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workload,
        const TBlock& block) override
    {
        Sink->WriteBlock(options, workload, block);
        return ReadyResult.IsSet() && ReadyResult.GetOrCrash().IsOK();
    }

    bool WriteBlocks(
        const TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workload,
        const std::vector<TBlock>& blocks) override
    {
        Sink->WriteBlocks(options, workload, blocks);
        return ReadyResult.IsSet() && ReadyResult.GetOrCrash().IsOK();
    }

    TFuture<void> GetReadyEvent() override
    {
        return ReadyResult;
    }

    TFuture<void> Close(
        const TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workload,
        const TDeferredChunkMetaPtr& meta) override
    {
        return CloseResult.Apply(BIND([this, this_ = MakeStrong(this), options, workload, meta] {
            return Sink->Close(options, workload, meta);
        }));
    }

    const TChunkInfo& GetChunkInfo() const override
    {
        return Sink->GetChunkInfo();
    }

    const TDataStatistics& GetDataStatistics() const override
    {
        return Sink->GetDataStatistics();
    }

    TWrittenChunkReplicasInfo GetWrittenChunkReplicasInfo() const override
    {
        return Sink->GetWrittenChunkReplicasInfo();
    }

    TChunkId GetChunkId() const override
    {
        return Sink->GetChunkId();
    }

    NErasure::ECodec GetErasureCodecId() const override
    {
        return Sink->GetErasureCodecId();
    }

    bool IsCloseDemanded() const override
    {
        return DemandClose;
    }

    TFuture<void> Cancel() override
    {
        return Sink->Cancel();
    }
};

class TTestChunkWriter
    : public IChunkWriterBase
{
public:
    i64 MetaSize = 0;
    i64 DataWeight = 0;

    explicit TTestChunkWriter(IChunkWriterPtr sink)
        : Sink_(std::move(sink))
    { }

    bool Write()
    {
        if (!Opened_) {
            WaitFor(Sink_->Open())
                .ThrowOnError();
            Opened_ = true;
        }
        ++BlockCount_;
        return Sink_->WriteBlock(
            /*options*/ {},
            /*workloadDescriptor*/ {},
            TBlock(TSharedRef::FromString(std::string("x"))));
    }

    TFuture<void> Close() override
    {
        TMiscExt misc;
        misc.set_row_count(BlockCount_);
        misc.set_data_weight(DataWeight);
        misc.set_compressed_data_size(BlockCount_);
        misc.set_uncompressed_data_size(BlockCount_);
        SetProtoExtension(Meta_->mutable_extensions(), misc);
        return Sink_->Close(/*options*/ {}, /*workloadDescriptor*/ {}, Meta_);
    }

    TFuture<void> GetReadyEvent() override
    {
        return Sink_->GetReadyEvent();
    }

    i64 GetMetaSize() const override
    {
        return MetaSize;
    }

    i64 GetCompressedDataSize() const override
    {
        return BlockCount_;
    }

    i64 GetDataWeight() const override
    {
        return DataWeight;
    }

    bool IsCloseDemanded() const override
    {
        return Sink_->IsCloseDemanded();
    }

    TDeferredChunkMetaPtr GetMeta() const override
    {
        return Meta_;
    }

    TChunkId GetChunkId() const override
    {
        return Sink_->GetChunkId();
    }

    TDataStatistics GetDataStatistics() const override
    {
        TDataStatistics result;
        result.set_row_count(BlockCount_);
        result.set_chunk_count(BlockCount_ > 0 ? 1 : 0);
        return result;
    }

    TCodecStatistics GetCompressionStatistics() const override
    {
        return {};
    }

private:
    const IChunkWriterPtr Sink_;
    const TDeferredChunkMetaPtr Meta_ = New<TDeferredChunkMeta>();
    bool Opened_ = false;
    i64 BlockCount_ = 0;
};

class TTestMultiWriter
    : public TNontemplateMultiChunkWriterBase
{
public:
    TIntrusivePtr<TTestChunkWriter> ChunkWriter;

    TTestMultiWriter(TMultiChunkWriterConfigPtr config, TUnderlyingChunkWriterFactory factory)
        : TNontemplateMultiChunkWriterBase(
            std::move(config),
            New<TMultiChunkWriterOptions>(),
            /*client*/ nullptr,
            /*localHostName*/ {},
            TCellTag(1),
            /*transactionId*/ {},
            /*schemaId*/ {},
            NullChunkListId,
            /*trafficMeter*/ nullptr,
            GetUnlimitedThrottler(),
            /*blockCache*/ nullptr,
            std::move(factory))
    { }

    bool Write()
    {
        bool ready = ChunkWriter->Write();
        bool switched = TrySwitchSession();
        return ready && !switched;
    }

private:
    IChunkWriterBasePtr CreateTemplateWriter(IChunkWriterPtr sink) override
    {
        ChunkWriter = New<TTestChunkWriter>(std::move(sink));
        return ChunkWriter;
    }
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EChunkRotationReason,
    (CloseDemand)
    (MetaSize)
    (DataWeight)
    (ChunkSize)
);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESinkFailureStage,
    (Open)
    (Ready)
    (Close)
);

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkWriterTest
    : public ::testing::Test
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_PI(
    TMultiChunkWriterTest,
    RotationThresholds,
    ::testing::ValuesIn(TEnumTraits<EChunkRotationReason>::GetDomainValues()))
{
    auto reason = GetParam();
    std::vector<TIntrusivePtr<TMockSink>> sinks;
    auto config = New<TMultiChunkWriterConfig>();
    config->MaxMetaSize = 10;
    config->DesiredChunkWeight = 10;
    config->DesiredChunkSize = reason == EChunkRotationReason::ChunkSize ? 1 : 100;
    auto writer = New<TTestMultiWriter>(
        config,
        BIND([&] () -> IChunkWriterPtr {
            auto sink = New<TMockSink>();
            sinks.push_back(sink);
            return sink;
        }));
    writer->ChunkWriter->MetaSize = 10;
    writer->ChunkWriter->DataWeight = 9;
    EXPECT_TRUE(writer->Write());
    EXPECT_EQ(1u, sinks.size());
    switch (reason) {
        case EChunkRotationReason::CloseDemand:
            sinks.front()->DemandClose = true;
            break;
        case EChunkRotationReason::MetaSize:
            writer->ChunkWriter->MetaSize = 11;
            break;
        case EChunkRotationReason::DataWeight:
            writer->ChunkWriter->DataWeight = 10;
            break;
        case EChunkRotationReason::ChunkSize:
            break;
    }

    EXPECT_FALSE(writer->Write());
    WaitFor(writer->GetReadyEvent())
        .ThrowOnError();
    EXPECT_EQ(2u, sinks.size());
    EXPECT_EQ(2, writer->GetDataStatistics().row_count());
    WaitFor(writer->Close())
        .ThrowOnError();
    EXPECT_EQ(1u, writer->GetWrittenChunkSpecs().size());
    EXPECT_EQ(2u, sinks.front()->Sink->GetBlocks().size());
}

TEST_F(TMultiChunkWriterTest, AcceptedButNotReady)
{
    auto sink = New<TMockSink>();
    auto ready = NewPromise<void>();
    sink->ReadyResult = ready.ToFuture();
    auto writer = New<TTestMultiWriter>(
        New<TMultiChunkWriterConfig>(),
        BIND([sink] () -> IChunkWriterPtr {
            return sink;
        }));
    EXPECT_FALSE(writer->Write());
    auto event = writer->GetReadyEvent();
    EXPECT_FALSE(event.IsSet());
    ready.Set();
    WaitFor(event)
        .ThrowOnError();
    EXPECT_TRUE(writer->Write());
    WaitFor(writer->Close())
        .ThrowOnError();
    EXPECT_EQ(2u, sink->Sink->GetBlocks().size());
}

TEST_PI(
    TMultiChunkWriterTest,
    SinkFailures,
    ::testing::ValuesIn(TEnumTraits<ESinkFailureStage>::GetDomainValues()))
{
    auto stage = GetParam();
    auto sink = New<TMockSink>();
    auto error = MakeFuture<void>(TError("controlled sink failure"));
    if (stage == ESinkFailureStage::Open) {
        sink->OpenResult = error;
    } else if (stage == ESinkFailureStage::Ready) {
        sink->ReadyResult = error;
    } else {
        sink->CloseResult = error;
    }

    auto writer = New<TTestMultiWriter>(
        New<TMultiChunkWriterConfig>(),
        BIND([sink] () -> IChunkWriterPtr {
            return sink;
        }));
    if (stage == ESinkFailureStage::Open) {
        EXPECT_THROW_WITH_SUBSTRING(writer->Write(), "controlled sink failure");
    } else if (stage == ESinkFailureStage::Ready) {
        EXPECT_FALSE(writer->Write());
        EXPECT_THROW_WITH_SUBSTRING(
            WaitFor(writer->GetReadyEvent())
                .ThrowOnError(),
            "controlled sink failure");
    } else {
        EXPECT_TRUE(writer->Write());
        EXPECT_THROW_WITH_SUBSTRING(
            WaitFor(writer->Close())
                .ThrowOnError(),
            "controlled sink failure");
        EXPECT_TRUE(writer->GetWrittenChunkSpecs().empty());
    }
}

TEST_F(TMultiChunkWriterTest, DestroyedBeforePendingClose)
{
    auto sink = New<TMockSink>();
    auto writer = New<TTestMultiWriter>(
        New<TMultiChunkWriterConfig>(),
        BIND([sink] () -> IChunkWriterPtr {
            return sink;
        }));
    EXPECT_TRUE(writer->Write());

    auto started = NewPromise<void>();
    // Block the writer thread; WaitFor would yield and let the queued close run.
    auto release = std::make_shared<std::binary_semaphore>(0);
    auto blocker = BIND([started, release] {
        started.Set();
        release->acquire();
    })
        .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
        .Run();
    WaitFor(started.ToFuture())
        .ThrowOnError();
    auto closed = writer->Close();
    writer.Reset();
    release->release();
    WaitFor(blocker)
        .ThrowOnError();
    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(closed)
            .ThrowOnError(),
        "Object destroyed");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

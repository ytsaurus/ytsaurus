#include <gtest/gtest.h>

#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/memory_writer.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>
#include <yt/yt/ytlib/chunk_client/striped_erasure_writer.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NChunkClient {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

struct TChunk
{
    using TPart = std::vector<TString>;
    std::vector<TPart> Parts;

    TRefCountedChunkMetaPtr Meta;
};

TChunk WriteErasureChunk(
    NErasure::ECodec codecId,
    const std::vector<TSharedRef>& blocks,
    TErasureWriterConfigPtr config = New<TErasureWriterConfig>())
{
    auto* codec = NErasure::GetCodec(codecId);
    auto totalPartCount = codec->GetTotalPartCount();

    std::vector<IChunkWriterPtr> writers;
    std::vector<TMemoryWriterPtr> memoryWriters;
    writers.reserve(totalPartCount);
    memoryWriters.reserve(totalPartCount);
    for (int index = 0; index < totalPartCount; ++index) {
        memoryWriters.push_back(New<TMemoryWriter>());
        writers.push_back(memoryWriters.back());
    }

    auto writer = CreateStripedErasureWriter(
        config,
        codecId,
        TSessionId(),
        TWorkloadDescriptor(),
        writers);

    WaitFor(writer->Open())
        .ThrowOnError();

    for (const auto& block : blocks) {
        if (!writer->WriteBlock(TWorkloadDescriptor(), TBlock(block))) {
            WaitFor(writer->GetReadyEvent())
                .ThrowOnError();
        }
    }

    auto deferredMeta = New<TDeferredChunkMeta>();
    SetProtoExtension(deferredMeta->mutable_extensions(), NProto::TMiscExt());
    WaitFor(writer->Close(TWorkloadDescriptor(), deferredMeta))
        .ThrowOnError();

    TChunk chunk;
    for (const auto& writer : memoryWriters) {
        auto blocks = writer->GetBlocks();
        std::vector<TString> part;
        for (const auto& block : blocks) {
            part.push_back(ToString(block.Data));
        }
        chunk.Parts.push_back(part);
    }
    chunk.Meta = memoryWriters.front()->GetChunkMeta();

    return chunk;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStripedErasureTest, WriterSimple)
{
    std::vector<TSharedRef> blocks = {
        TSharedRef::FromString("abc"),
        TSharedRef::FromString(""),
        TSharedRef::FromString("defg"),
        TSharedRef::FromString("hijkl"),
    };

    auto chunk = WriteErasureChunk(NErasure::ECodec::ReedSolomon_3_3, blocks);

    EXPECT_EQ(chunk.Parts[0], std::vector<TString>({"a", "", "de", "hi"}));
    EXPECT_EQ(chunk.Parts[1], std::vector<TString>({"b", "", "fg", "jk"}));

    TString LNull = "l";
    LNull.push_back('\0');
    TString NullNull;
    NullNull.push_back('\0');
    NullNull.push_back('\0');
    EXPECT_EQ(chunk.Parts[2], std::vector<TString>({"c", "", NullNull, LNull}));
}

TEST(TStripedErasureTest, SegmentsSimple)
{
    std::vector<TSharedRef> blocks = {
        TSharedRef::FromString("abc"),
        TSharedRef::FromString("def"),
        TSharedRef::FromString("ghi"),
        TSharedRef::FromString("jkl"),
    };

    auto config = New<TErasureWriterConfig>();
    config->DesiredSegmentPartSize = 1000;

    auto chunk = WriteErasureChunk(NErasure::ECodec::ReedSolomon_3_3, blocks, config);

    EXPECT_EQ(chunk.Parts[0], std::vector<TString>({"abcd"}));
    EXPECT_EQ(chunk.Parts[1], std::vector<TString>({"efgh"}));
    EXPECT_EQ(chunk.Parts[2], std::vector<TString>({"ijkl"}));
}

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((InterceptingMemoryWriterFailure) (123))
);

DECLARE_REFCOUNTED_CLASS(TInterceptingMemoryWriter)

class TInterceptingMemoryWriter
    : public TMemoryWriter
{
public:
    explicit TInterceptingMemoryWriter(bool shouldIntercept)
        : ShouldIntercept_(shouldIntercept)
    { }

    bool WriteBlock(
        const TWorkloadDescriptor& workloadDescriptor,
        const TBlock& block) override
    {
        if (!ShouldIntercept_) {
            return TMemoryWriter::WriteBlock(workloadDescriptor, block);
        }

        WriteRequested_.Set();

        return false;
    }

    TFuture<void> GetReadyEvent() override
    {
        if (!ShouldIntercept_) {
            return TMemoryWriter::GetReadyEvent();
        }

        return ReadyEvent_;
    }

    TFuture<void> GetWriteRequestedFuture()
    {
        return WriteRequested_;
    }

    void StopInterceptingAndFail()
    {
        if (!ShouldIntercept_) {
            return;
        }

        YT_VERIFY(!ReadyEvent_.IsSet());

        ReadyEvent_.Set(TError(
            EErrorCode::InterceptingMemoryWriterFailure,
            "Intercepting writer error"));
    }

private:
    const bool ShouldIntercept_;

    const TPromise<void> WriteRequested_ = NewPromise<void>();

    const TPromise<void> ReadyEvent_ = NewPromise<void>();
};

DEFINE_REFCOUNTED_TYPE(TInterceptingMemoryWriter)

TEST(TStripedErasureTest, FailureWhileClosing)
{
    auto* codec = NErasure::GetCodec(NErasure::ECodec::ReedSolomon_3_3);
    auto totalPartCount = codec->GetTotalPartCount();

    std::vector<IChunkWriterPtr> writers;
    std::vector<TInterceptingMemoryWriterPtr> memoryWriters;
    writers.reserve(totalPartCount);
    memoryWriters.reserve(totalPartCount);
    for (int index = 0; index < totalPartCount; ++index) {
        memoryWriters.push_back(New<TInterceptingMemoryWriter>(
            /*shouldIntercept*/ index == 0));
        writers.push_back(memoryWriters.back());
    }

    auto interceptingWriter = memoryWriters[0];

    auto config = New<TErasureWriterConfig>();
    config->WriterGroupSize = 1;
    config->WriterWindowSize = 1;

    auto writer = CreateStripedErasureWriter(
        config,
        codec->GetId(),
        TSessionId(),
        TWorkloadDescriptor(),
        writers);

    WaitFor(writer->Open())
        .ThrowOnError();

    TSharedRef block = TSharedRef::FromString("abcd");
    EXPECT_FALSE(writer->WriteBlock(TWorkloadDescriptor(), TBlock(block)));
    // First ready event will always be successful regardless of intercepting writer malfunction.
    WaitFor(writer->GetReadyEvent())
        .ThrowOnError();

    // Longer string is used because the writer's window size accounting is imprecise.
    block = TSharedRef::FromString("efghefghefghefghefgh");
    EXPECT_FALSE(writer->WriteBlock(TWorkloadDescriptor(), TBlock(block)));
    auto readyEvent = writer->GetReadyEvent();
    // This ready event is supposed to be blocked on intercepting writer's ready event.
    EXPECT_FALSE(readyEvent.IsSet());

    // For testing purposes it is important that erasure writer uses serialized invoker
    // so this action will be executed after the second flush.
    auto deferredMeta = New<TDeferredChunkMeta>();
    SetProtoExtension(deferredMeta->mutable_extensions(), NProto::TMiscExt());
    auto closeFuture = writer->Close(TWorkloadDescriptor(), deferredMeta);
    EXPECT_FALSE(closeFuture.IsSet());

    // Best effort to ensure that these actions above were actually invoked.
    Sleep(TDuration::Seconds(10));

    EXPECT_FALSE(readyEvent.IsSet());
    interceptingWriter->StopInterceptingAndFail();
    auto readyEventResult = WaitFor(readyEvent);
    EXPECT_FALSE(readyEventResult.IsOK());
    EXPECT_TRUE(readyEventResult.FindMatching(EErrorCode::InterceptingMemoryWriterFailure));

    auto closeResult = WaitFor(closeFuture);
    EXPECT_FALSE(closeResult.IsOK());
    EXPECT_TRUE(closeResult.FindMatching(EErrorCode::InterceptingMemoryWriterFailure));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkClient

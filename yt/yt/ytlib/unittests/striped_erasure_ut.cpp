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
        if (!writer->WriteBlock(TBlock(block))) {
            WaitFor(writer->GetReadyEvent())
                .ThrowOnError();
        }
    }

    auto deferredMeta = New<TDeferredChunkMeta>();
    SetProtoExtension(deferredMeta->mutable_extensions(), NProto::TMiscExt());
    WaitFor(writer->Close(deferredMeta))
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

} // namespace
} // namespace NYT::NChunkClient

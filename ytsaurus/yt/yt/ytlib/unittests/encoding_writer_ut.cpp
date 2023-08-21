#include <algorithm>
#include <iterator>
#include <random>
#include <util/datetime/base.h>
#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/profiling/timing.h>
#include <yt/yt/client/table_client/config.h>

#include <yt/yt/ytlib/chunk_client/encoding_writer.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/memory_writer.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>

namespace NYT::NChunkClient {
namespace {

using namespace NConcurrency;
using namespace NCompression;

////////////////////////////////////////////////////////////////////////////////

template <class Comparer>
void DoTestEncodingWriter(
    const TEncodingWriterConfigPtr& config,
    const TEncodingWriterOptionsPtr& options,
    Comparer expectedTimeComparer)
{
    std::mt19937 rnd(42);
    auto memoryWriter = New<TMemoryWriter>();
    auto encodingWriter = New<TEncodingWriter>(
        config,
        options,
        memoryWriter,
        GetNullBlockCache(),
        NLogging::TLogger{"test"});

    std::vector<TSharedRef> blocks;
    int blockNum = 20;
    int blockSize =  1000000;
    blocks.reserve(blockNum);
    TString base;
    base.reserve(blockSize);
    TString whole;
    whole.reserve(blockNum * blockSize);
    std::generate_n(std::back_inserter(base), blockSize, [&rnd] {
        return rnd() % 127 + 1;
    });
    std::generate_n(std::back_inserter(blocks), blockNum, [base, &rnd, &whole]() mutable {
        base[rnd() % base.size()] = rnd() % 127 + 1;
        whole += base;
        return TSharedRef::FromString(base);
    });

    NProfiling::TWallTimer wallTimer;

    for (const auto& block : blocks) {
        while (!encodingWriter->IsReady()) {
            WaitFor(encodingWriter->GetReadyEvent())
                .ThrowOnError();
        }
        encodingWriter->WriteBlock(block, EBlockType::UncompressedData);
    }
    encodingWriter->WriteBlock(blocks, EBlockType::UncompressedData);

    WaitFor(encodingWriter->Flush())
        .ThrowOnError();

    auto wallTime = wallTimer.GetElapsedTime();

    WaitFor(memoryWriter->Close(TWorkloadDescriptor(), New<TDeferredChunkMeta>()))
        .ThrowOnError();

    auto result = memoryWriter->GetBlocks();

    auto codec = NCompression::GetCodec(options->CompressionCodec);
    EXPECT_EQ(std::ssize(blocks) + 1, std::ssize(result));
    for (ssize_t i = 0; i < std::ssize(blocks); ++i) {
        EXPECT_TRUE(TRef::AreBitwiseEqual(blocks[i], codec->Decompress(result[i].Data)));
    }
    EXPECT_TRUE(TRef::AreBitwiseEqual(
        TSharedRef::FromString(whole),
        codec->Decompress(result.back().Data)));
    EXPECT_TRUE(expectedTimeComparer(wallTime, encodingWriter->GetCompressionDuration().CpuDuration));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TEncodingWriterTest, WithoutConcurrency)
{
    auto options = New<TEncodingWriterOptions>();
    auto config = New<TEncodingWriterConfig>();
    config->CompressionConcurrency = 1;
    config->EncodeWindowSize = 8_MB;
    options->CompressionCodec = ECodec::GzipNormal;
    DoTestEncodingWriter(config, options, std::greater<TDuration>());
}

TEST(TEncodingWriterTest, WithConcurrency)
{
    auto options = New<TEncodingWriterOptions>();
    auto config = New<TEncodingWriterConfig>();
    config->CompressionConcurrency = 2;
    config->EncodeWindowSize = 8_MB;
    options->CompressionCodec = ECodec::GzipNormal;
    DoTestEncodingWriter(config, options, std::less<TDuration>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkClient

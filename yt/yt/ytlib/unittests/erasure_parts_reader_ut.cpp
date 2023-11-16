#include <yt/yt/library/erasure/impl/public.h>
#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/private.h>

#include <yt/yt/ytlib/journal_client/helpers.h>
#include <yt/yt/ytlib/journal_client/erasure_parts_reader.h>

#include <yt/yt/client/journal_client/config.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/test_framework/framework.h>

#include <util/random/fast.h>

#include <numeric>

namespace NYT::NChunkClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TSharedRef GenerateRandomBlock(TFastRng64& rng, int blockSize)
{
    TString result;
    result.resize(blockSize);
    for (auto& ch : result) {
        ch = rng.Uniform(33, 127);
    }
    return TSharedRef::FromString(std::move(result));
}

std::vector<TSharedRef> GenerateRandomRows(TFastRng64& rng, i64 rowCount, i64 rowSizeBytes)
{
    std::vector<TSharedRef> result(rowCount);
    for (auto& block : result) {
        block = GenerateRandomBlock(rng, rowSizeBytes);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

enum class ERequestBehavior
{
    Succeed,
    Fail,
    Slow,
    FailImmediately,
};

struct TPlot
{
    ERequestBehavior GetMetaBehavior = ERequestBehavior::Succeed;
    ERequestBehavior ReadBlocksBehavior = ERequestBehavior::Succeed;
    std::optional<int> MaxBlockCount;
};

class TMockChunkReader
    : public IChunkReader
{
public:
    TMockChunkReader(TChunkId chunkId, std::vector<TSharedRef> blocks, TPlot plot)
        : ChunkId_(chunkId)
        , Blocks_ (std::move(blocks))
        , ActionQueue_(New<NConcurrency::TActionQueue>())
        , Invoker_(ActionQueue_->GetInvoker())
        , Plot_(plot)
    { }

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& /*options*/,
        const std::vector<int>& /*blockIndexes*/) override
    {
        YT_ABORT();
    }

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& /*options*/,
        int firstBlockIndex,
        int blockCount) override
    {
        if (Plot_.ReadBlocksBehavior == ERequestBehavior::FailImmediately) {
            return MakeFuture<std::vector<TBlock>>(TError("Failed Inline"));
        }

        return BIND(&TMockChunkReader::DoReadBlocks, MakeStrong(this), firstBlockIndex, blockCount)
            .AsyncVia(Invoker_)
            .Run();
    }

    virtual TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientChunkReadOptions& /*options*/,
        std::optional<int> /*partitionTag*/,
        const std::optional<std::vector<int>>& /*extensionTags*/) override
    {
        if (Plot_.GetMetaBehavior == ERequestBehavior::FailImmediately) {
            return MakeFuture<TRefCountedChunkMetaPtr>(TError("Failed Inline"));
        }

        return BIND(&TMockChunkReader::DoGetMeta, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
    }

    virtual TChunkId GetChunkId() const override
    {
        return ChunkId_;
    }

    virtual TInstant GetLastFailureTime() const override
    {
        return {};
    }

private:
    const TChunkId ChunkId_;
    const std::vector<TSharedRef> Blocks_;
    const NConcurrency::TActionQueuePtr ActionQueue_;
    const IInvokerPtr Invoker_;
    const TPlot Plot_;


    std::vector<TBlock> DoReadBlocks(int firstBlockIndex, int blockCount)
    {
        ExecBehavior(Plot_.ReadBlocksBehavior);
        if (Plot_.MaxBlockCount) {
            YT_VERIFY(*Plot_.MaxBlockCount >= blockCount);
        }

        std::vector<TBlock> blocks;
        for (int index = firstBlockIndex; index < firstBlockIndex + blockCount; ++index) {
            blocks.emplace_back(Blocks_[index]);
        }
        return blocks;
    }

    TRefCountedChunkMetaPtr DoGetMeta()
    {
        ExecBehavior(Plot_.GetMetaBehavior);

        auto chunkMeta = New<TRefCountedChunkMeta>();

        NChunkClient::NProto::TMiscExt miscExt;
        if (Plot_.MaxBlockCount) {
            miscExt.set_row_count(std::min<int>(std::ssize(Blocks_), *Plot_.MaxBlockCount));
        } else {
            miscExt.set_row_count(Blocks_.size());
        }
        i64 dataSize = 0;
        for (const auto& buffer : Blocks_) {
            dataSize += buffer.Size();
        }
        miscExt.set_uncompressed_data_size(dataSize);
        SetProtoExtension(chunkMeta->mutable_extensions(), miscExt);

        return chunkMeta;
    }

    void ExecBehavior(ERequestBehavior behavior)
    {
        switch (behavior) {
            case ERequestBehavior::Succeed:
                break;

            case ERequestBehavior::Fail:
                THROW_ERROR_EXCEPTION("Request failed!");

            case ERequestBehavior::Slow:
                NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
                break;

            default:
                YT_ABORT();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestCase
{
    int FirstRowIndex = 7;
    int RowCount = 1;

    NErasure::TPartIndexList RequestedPartIndices = {0, 1, 2};
    std::vector<int> ExpectedPartRowCount = {1, 1, 1};
    int DecodeRowCount = 1;

    THashMap<int, TPlot> ReadersPlot;

    bool ShouldFail = false;
};

////////////////////////////////////////////////////////////////////////////////

constexpr int TotalRowCount = 11;
constexpr int RowByteSize = 513;

void ExecTest(TTestCase testCase)
{
    TFastRng64 rng(27);
    const auto rows = GenerateRandomRows(rng, TotalRowCount, RowByteSize);

    auto* codec = NErasure::GetCodec(NErasure::ECodec::IsaReedSolomon_3_3);
    auto encodedRows = NJournalClient::EncodeErasureJournalRows(codec, rows);

    EXPECT_EQ(encodedRows.size(), 6u);

    auto chunkId = NObjectClient::MakeRandomId(
        NObjectClient::EObjectType::ErasureJournalChunk,
        NObjectClient::TCellTag(0));
    std::vector<NChunkClient::IChunkReaderPtr> chunkReadersList;
    for (int part = 0; part < std::ssize(encodedRows); ++part) {
        auto replicaId = NChunkClient::EncodeChunkId({chunkId, part});
        chunkReadersList.push_back(New<TMockChunkReader>(replicaId, encodedRows[part], testCase.ReadersPlot[part]));
    }

    auto config = New<NJournalClient::TChunkReaderConfig>();
    auto reader = New<NJournalClient::TErasurePartsReader>(
        config,
        codec,
        chunkReadersList,
        testCase.RequestedPartIndices,
        NChunkClient::ChunkClientLogger);

    auto rowParts = reader->ReadRows(
        /*options*/ {},
        testCase.FirstRowIndex,
        testCase.RowCount)
        .Get()
        .ValueOrThrow();

    ASSERT_EQ(rowParts.size(), testCase.ExpectedPartRowCount.size());
    for (int partIndex = 0; partIndex < std::ssize(rowParts); ++partIndex) {
        ASSERT_GE(std::ssize(rowParts[partIndex]), testCase.ExpectedPartRowCount[partIndex]);
        for (int row = 0; row < testCase.ExpectedPartRowCount[partIndex]; ++row) {
            EXPECT_TRUE(TRef::AreBitwiseEqual(encodedRows[partIndex][row + testCase.FirstRowIndex], rowParts[partIndex][row]));
        }
    }

    if (testCase.DecodeRowCount > 0) {
        auto decodedRows = NJournalClient::DecodeErasureJournalRows(codec, rowParts);
        ASSERT_GE(std::ssize(decodedRows), testCase.DecodeRowCount);
        for (int row = 0; row < testCase.DecodeRowCount; ++row) {
            EXPECT_TRUE(TRef::AreBitwiseEqual(rows[row + testCase.FirstRowIndex], decodedRows[row]));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TErasurePartsReaderTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<TTestCase>
{ };

TEST_P(TErasurePartsReaderTest, SimpleTest)
{
    auto testCase = GetParam();
    if (testCase.ShouldFail) {
        EXPECT_THROW(ExecTest(testCase), std::exception);
    } else {
        ExecTest(testCase);
    }
}

INSTANTIATE_TEST_SUITE_P(
    TErasurePartsReaderTest,
    TErasurePartsReaderTest,
    ::testing::Values(
        TTestCase{
            .FirstRowIndex = 7,
        },
        TTestCase{
            .FirstRowIndex = 0,
            .RowCount = TotalRowCount,
            .ExpectedPartRowCount = {TotalRowCount, TotalRowCount, TotalRowCount},
            .DecodeRowCount = TotalRowCount,
        },
        TTestCase{
            .FirstRowIndex = 10,
            .RowCount = 1,
        },
        TTestCase{
            .FirstRowIndex = 11,
            .RowCount = 1,
            .ShouldFail = true,
        },
        TTestCase{
            .FirstRowIndex = 3,
            .RowCount = 3,
            .ExpectedPartRowCount = {3, 3, 3},
            .DecodeRowCount = 3,
            .ReadersPlot = {
                {0, {.GetMetaBehavior = ERequestBehavior::Fail}},
                {1, {.GetMetaBehavior = ERequestBehavior::Fail}},
                {2, {.GetMetaBehavior = ERequestBehavior::Fail}},
            },
        },
        TTestCase{
            .ReadersPlot = {
                {0, {.GetMetaBehavior = ERequestBehavior::Fail}},
                {1, {.GetMetaBehavior = ERequestBehavior::Fail}},
                {2, {.GetMetaBehavior = ERequestBehavior::Fail}},
                {3, {.GetMetaBehavior = ERequestBehavior::Fail}},
            },
            .ShouldFail = true,
        },
        TTestCase{
            .ReadersPlot = {
                {0, {.GetMetaBehavior = ERequestBehavior::Slow}},
                {1, {.GetMetaBehavior = ERequestBehavior::Slow}},
                {2, {.GetMetaBehavior = ERequestBehavior::Slow}},
            },
        },
        TTestCase{
            .ReadersPlot = {
                {0, {.GetMetaBehavior = ERequestBehavior::Slow}},
                {1, {.GetMetaBehavior = ERequestBehavior::Slow}},
                {2, {.GetMetaBehavior = ERequestBehavior::Slow}},
                {3, {.GetMetaBehavior = ERequestBehavior::Fail}},
                {4, {.GetMetaBehavior = ERequestBehavior::Fail}},
                {5, {.GetMetaBehavior = ERequestBehavior::Fail}},
            },
        },
        TTestCase{
            .ReadersPlot = {
                {0, {.GetMetaBehavior = ERequestBehavior::Slow}},
                {1, {.GetMetaBehavior = ERequestBehavior::Slow}},
                {2, {.GetMetaBehavior = ERequestBehavior::Slow}},
                {3, {.ReadBlocksBehavior = ERequestBehavior::Fail}},
                {4, {.ReadBlocksBehavior = ERequestBehavior::Fail}},
                {5, {.ReadBlocksBehavior = ERequestBehavior::Fail}},
            },
        },
        TTestCase{
            .ReadersPlot = {
                {0, {.ReadBlocksBehavior = ERequestBehavior::Fail}},
                {1, {.ReadBlocksBehavior = ERequestBehavior::Fail}},
                {2, {.ReadBlocksBehavior = ERequestBehavior::Fail}},
            },
        },
        TTestCase{
            .RequestedPartIndices = {0},
            .ExpectedPartRowCount = {1},
            .DecodeRowCount = 0,
            .ReadersPlot = {
                {0, {.GetMetaBehavior = ERequestBehavior::Slow}},
                {1, {.GetMetaBehavior = ERequestBehavior::Fail}},
                {2, {.GetMetaBehavior = ERequestBehavior::Fail}},
                {3, {.GetMetaBehavior = ERequestBehavior::Fail}},
                {4, {.GetMetaBehavior = ERequestBehavior::Fail}},
                {5, {.GetMetaBehavior = ERequestBehavior::Fail}},
            },
        },
        TTestCase{
            .FirstRowIndex = 10,
            .ReadersPlot = {
                {0, {.MaxBlockCount = 9}},
                {1, {.MaxBlockCount = 9}},
                {2, {.MaxBlockCount = 9}},
                {3, {.MaxBlockCount = 9}},
            },
            .ShouldFail = true
        },
        TTestCase{
            .FirstRowIndex = 10,
            .ReadersPlot = {
                {1, {.MaxBlockCount = 9}},
                {2, {.MaxBlockCount = 9}},
                {3, {.MaxBlockCount = 9}},
            },
        },
        TTestCase{
            .FirstRowIndex = 10,
            .RequestedPartIndices = {0},
            .ExpectedPartRowCount = {1},
            .DecodeRowCount = 0,
            .ReadersPlot = {
                {0, {.GetMetaBehavior = ERequestBehavior::Slow}},
                {1, {.MaxBlockCount = 9}},
                {2, {.MaxBlockCount = 9}},
                {3, {.MaxBlockCount = 9}},
                {4, {.MaxBlockCount = 9}},
            },
        },
        TTestCase{
            .FirstRowIndex = 3,
            .RowCount = 3,
            .ExpectedPartRowCount = {3, 3, 3},
            .DecodeRowCount = 3,
            .ReadersPlot = {
                {0, {.GetMetaBehavior = ERequestBehavior::FailImmediately}},
                {1, {.GetMetaBehavior = ERequestBehavior::FailImmediately}},
                {2, {.GetMetaBehavior = ERequestBehavior::FailImmediately}},
            },
        }));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkClient

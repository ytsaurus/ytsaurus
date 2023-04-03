#include <yt/yt/library/erasure/impl/public.h>
#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/journal_client/helpers.h>
#include <yt/yt/ytlib/journal_client/erasure_parts_reader.h>

#include <yt/yt/client/journal_client/config.h>
#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/misc/error.h>
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

struct TMockChunkReader
    : public IChunkReader
{
    struct TPlot
    {
        enum ERequestBehavior
        {
            Succeed,
            Fail,
            Slow,
            FailImmediate
        };

        ERequestBehavior GetMetaBehavior = Succeed;
        ERequestBehavior ReadBlocksBehavior = Succeed;
        std::optional<int> MaxBlockCount;
    };

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
        if (Plot_.ReadBlocksBehavior == TPlot::FailImmediate) {
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
        if (Plot_.GetMetaBehavior == TPlot::FailImmediate) {
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
            blocks.emplace_back(Blocks_.at(index));
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

    void ExecBehavior(TPlot::ERequestBehavior behavior)
    {
        switch (behavior)
        {
            case TPlot::Succeed:
                break;

            case TPlot::Fail:
                THROW_ERROR_EXCEPTION("Request failed!");

            case TPlot::Slow:
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
    using TRowIndex = int;
    TRowIndex FirstRowIndex = 7;
    int RowCount = 1;

    NErasure::TPartIndexList RequestedParts = {0, 1, 2};
    std::vector<int> ExpectedPartRowsCount = {1, 1, 1};
    int DecodeRowsCount = 1;

    THashMap<int, TMockChunkReader::TPlot> ReadersPlot;

    TDuration SlowPathDelay = TDuration::Seconds(5);

    bool ShouldFail = false;
};

////////////////////////////////////////////////////////////////////////////////

const int TotalRowsCount = 11;
const int RowSizeBytes = 513;

void ExecTest(TTestCase testCase)
{
    TFastRng64 rng(27);
    const auto rows = GenerateRandomRows(rng, TotalRowsCount, RowSizeBytes);

    auto* codec = NErasure::GetCodec(NErasure::ECodec::IsaReedSolomon_3_3);
    auto encodedRows = NJournalClient::EncodeErasureJournalRows(codec, rows);

    EXPECT_EQ(encodedRows.size(), 6u);

    const auto chunkId = NObjectClient::MakeRandomId(NObjectClient::EObjectType::ErasureJournalChunk, 0);
    std::vector<NChunkClient::IChunkReaderPtr> chunkReadersList;
    for (int part = 0; part < std::ssize(encodedRows); ++part) {
        auto replicaId = NChunkClient::EncodeChunkId({chunkId, part});
        chunkReadersList.push_back(New<TMockChunkReader>(replicaId, encodedRows[part], testCase.ReadersPlot[part]));
    }

    NLogging::TLogger logger{"erasure_reader"};

    auto config = New<NJournalClient::TChunkReaderConfig>();
    config->SlowPathDelay = testCase.SlowPathDelay;
    auto reader = New<NJournalClient::TErasurePartsReader>(config,
        codec,
        chunkReadersList,
        testCase.RequestedParts,
        logger);

    auto rowParts = reader->ReadRows(
        /*options*/ {},
        testCase.FirstRowIndex,
        testCase.RowCount,
        /*enableFastPath*/ true)
        .Get()
        .ValueOrThrow();
    EXPECT_EQ(rowParts.size(), testCase.ExpectedPartRowsCount.size());
    for (int partIndex = 0; partIndex < std::ssize(rowParts); ++partIndex) {
        for (int row = 0; row <  testCase.ExpectedPartRowsCount[partIndex]; ++row) {
            EXPECT_TRUE(TRef::AreBitwiseEqual(encodedRows[partIndex][row + testCase.FirstRowIndex], rowParts[partIndex][row]));
        }
    }

    if (testCase.DecodeRowsCount) {
        auto decodedRows = NJournalClient::DecodeErasureJournalRows(codec, rowParts);
        for (int row = 0; row < testCase.DecodeRowsCount; ++row) {
            EXPECT_TRUE(TRef::AreBitwiseEqual(rows[row + testCase.FirstRowIndex], decodedRows[row]));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TErasurePartsReaderTest, SimpleTest)
{
    using TPlot = TMockChunkReader::TPlot;
    const std::vector<TTestCase> TestCases {
        {
            .FirstRowIndex = 7,
        },
        {
            .FirstRowIndex = 0,
            .RowCount = TotalRowsCount,
            .ExpectedPartRowsCount = {TotalRowsCount, TotalRowsCount, TotalRowsCount},
            .DecodeRowsCount = TotalRowsCount
        },
        {
            .FirstRowIndex = 10,
            .RowCount = 1,
        },
        {
            .FirstRowIndex = 11,
            .RowCount = 1,
            .ShouldFail = true
        },
        {
            .FirstRowIndex = 3,
            .RowCount = 3,
            .ExpectedPartRowsCount = {3, 3, 3},
            .DecodeRowsCount = 3,
            .ReadersPlot = {
                {0, {.GetMetaBehavior = TPlot::Fail}},
                {1, {.GetMetaBehavior = TPlot::Fail}},
                {2, {.GetMetaBehavior = TPlot::Fail}},
            }
        },
        {
            .ReadersPlot = {
                {0, {.GetMetaBehavior = TPlot::Fail}},
                {1, {.GetMetaBehavior = TPlot::Fail}},
                {2, {.GetMetaBehavior = TPlot::Fail}},
                {3, {.GetMetaBehavior = TPlot::Fail}},
            },
            .ShouldFail = true
        },
        {
            .ReadersPlot = {
                {0, {.GetMetaBehavior = TPlot::Slow}},
                {1, {.GetMetaBehavior = TPlot::Slow}},
                {2, {.GetMetaBehavior = TPlot::Slow}},
            },
            .SlowPathDelay = TDuration::MilliSeconds(1),
            .ShouldFail = false,
        },
        {
            .ReadersPlot = {
                {0, {.GetMetaBehavior = TPlot::Slow}},
                {1, {.GetMetaBehavior = TPlot::Slow}},
                {2, {.GetMetaBehavior = TPlot::Slow}},
                {3, {.GetMetaBehavior = TPlot::Fail}},
                {4, {.GetMetaBehavior = TPlot::Fail}},
                {5, {.GetMetaBehavior = TPlot::Fail}},
            },
            .SlowPathDelay = TDuration::MilliSeconds(1),
            .ShouldFail = false,
        },
        // {
        //     .ReadersPlot = {
        //         {0, {.GetMetaBehavior = TPlot::Slow}},
        //         {1, {.GetMetaBehavior = TPlot::Slow}},
        //         {2, {.GetMetaBehavior = TPlot::Slow}},
        //         {3, {.ReadBlocksBehavior = TPlot::Fail}},
        //         {4, {.ReadBlocksBehavior = TPlot::Fail}},
        //         {5, {.ReadBlocksBehavior = TPlot::Fail}},
        //     },
        //     .SlowPathDelay = TDuration::MilliSeconds(1),
        //     .ShouldFail = false,
        // },
        // {
        //     .ReadersPlot = {
        //         {0, {.ReadBlocksBehavior = TPlot::Fail}},
        //         {1, {.ReadBlocksBehavior = TPlot::Fail}},
        //         {2, {.ReadBlocksBehavior = TPlot::Fail}},
        //     }
        // },
        {
            .RequestedParts = {0},
            .ExpectedPartRowsCount = {1},
            .DecodeRowsCount = 0,
            .ReadersPlot = {
                {0, {.GetMetaBehavior = TPlot::Slow}},
                {1, {.GetMetaBehavior = TPlot::Fail}},
                {2, {.GetMetaBehavior = TPlot::Fail}},
                {3, {.GetMetaBehavior = TPlot::Fail}},
                {4, {.GetMetaBehavior = TPlot::Fail}},
                {5, {.GetMetaBehavior = TPlot::Fail}},
            },
            .SlowPathDelay = TDuration::MilliSeconds(1),
            .ShouldFail = false,
        },
        {
            .FirstRowIndex = 10,
            .ReadersPlot = {
                {0, {.MaxBlockCount = 9}},
                {1, {.MaxBlockCount = 9}},
                {2, {.MaxBlockCount = 9}},
                {3, {.MaxBlockCount = 9}},
            },
            .ShouldFail = true
        },
        {
            .FirstRowIndex = 10,
            .ReadersPlot = {
                {1, {.MaxBlockCount = 9}},
                {2, {.MaxBlockCount = 9}},
                {3, {.MaxBlockCount = 9}},
            },
            .ShouldFail = false
        },
        {
            .FirstRowIndex = 10,
            .RequestedParts = {0},
            .ExpectedPartRowsCount = {1},
            .DecodeRowsCount = 0,
            .ReadersPlot = {
                {0, {.GetMetaBehavior = TPlot::Slow}},
                {1, {.MaxBlockCount = 9}},
                {2, {.MaxBlockCount = 9}},
                {3, {.MaxBlockCount = 9}},
                {4, {.MaxBlockCount = 9}},
            },
            .SlowPathDelay = TDuration::MilliSeconds(1),
            .ShouldFail = false,
        },
        {
            .FirstRowIndex = 3,
            .RowCount = 3,
            .ExpectedPartRowsCount = {3, 3, 3},
            .DecodeRowsCount = 3,
            .ReadersPlot = {
                {0, {.GetMetaBehavior = TPlot::FailImmediate}},
                {1, {.GetMetaBehavior = TPlot::FailImmediate}},
                {2, {.GetMetaBehavior = TPlot::FailImmediate}},
            }
        },
    };

    for (const auto& testCase : TestCases) {
        if (testCase.ShouldFail) {
            EXPECT_THROW(ExecTest(testCase), std::exception);
        } else {
            ExecTest(testCase);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkClient

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/controller_agent/helpers.h>

#include <yt/yt/server/lib/chunk_pools/input_chunk_mapping.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>

#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/misc/blob_output.h>

#include <random>

namespace NYT::NControllerAgent {
namespace {

using namespace NChunkClient;
using namespace NChunkPools;
using namespace NTableClient;

using NControllerAgent::TCompletedJobSummary;

using namespace ::testing;

////////////////////////////////////////////////////////////////////////////////

class TInputChunkMappingTest
    : public Test
{
protected:
    TInputChunkMappingPtr ChunkMapping_;

    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();

    EChunkMappingMode Mode_;

    //! Such comparator is enough for all keys in this test suite.
    const TComparator Comparator_ = TComparator(std::vector<ESortOrder>(2, ESortOrder::Ascending));

    NLogging::TLogger Logger = NLogging::TLogger("InputChunkMapping");

    void InitChunkMapping(EChunkMappingMode mode)
    {
        Mode_ = mode;
        ChunkMapping_ = New<TInputChunkMapping>(mode, Logger);
    }

    // In this test we will only deal with integral rows as
    // all the logic inside sorted chunk pool does not depend on
    // actual type of values in keys.
    // TODO(max42): extract to common helper base.
    TLegacyKey BuildRow(std::vector<i64> values)
    {
        auto row = RowBuffer_->AllocateUnversioned(values.size());
        for (int index = 0; index < std::ssize(values); ++index) {
            row[index] = MakeUnversionedInt64Value(values[index], index);
        }
        return row;
    }

    // TODO(max42): extract to common helper base.
    TInputChunkPtr CreateChunk(
        const TLegacyKey& minBoundaryKey = TLegacyKey(),
        const TLegacyKey& maxBoundaryKey = TLegacyKey(),
        i64 rowCount = 1000,
        i64 size = 1_KB)
    {
        auto inputChunk = New<TInputChunk>();
        inputChunk->SetChunkId(TChunkId::Create());
        inputChunk->SetCompressedDataSize(size);
        inputChunk->SetTotalUncompressedDataSize(size);
        inputChunk->SetTotalDataWeight(size);
        inputChunk->BoundaryKeys() = std::make_unique<TOwningBoundaryKeys>(TOwningBoundaryKeys {
            TLegacyOwningKey(minBoundaryKey),
            TLegacyOwningKey(maxBoundaryKey)
        });
        inputChunk->SetTotalRowCount(rowCount);
        return inputChunk;
    }

    TChunkStripePtr CreateStripe(const std::vector<TInputChunkPtr>& chunks)
    {
        std::vector<TLegacyDataSlicePtr> dataSlices;
        for (const auto& chunk : chunks) {
            auto dataSlice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
            dataSlice->SetInputStreamIndex(dataSlice->GetTableIndex());
            InferLimitsFromBoundaryKeys(dataSlice, RowBuffer_);
            dataSlice->TransformToNew(RowBuffer_, Comparator_);
            dataSlices.emplace_back(std::move(dataSlice));
        }
        auto stripe = New<TChunkStripe>();
        std::move(dataSlices.begin(), dataSlices.end(), std::back_inserter(stripe->DataSlices));
        return stripe;
    }

    TInputChunkPtr CopyChunk(const TInputChunkPtr& chunk)
    {
        auto chunkCopy = New<TInputChunk>();
        chunkCopy->SetChunkId(chunk->GetChunkId());
        chunkCopy->SetCompressedDataSize(chunk->GetCompressedDataSize());
        chunkCopy->BoundaryKeys() = std::make_unique<TOwningBoundaryKeys>(*chunk->BoundaryKeys());
        chunkCopy->SetTotalRowCount(chunk->GetRowCount());
        return chunkCopy;
    }

    std::vector<TInputChunkPtr> ToChunks(const TChunkStripePtr& stripe)
    {
        std::vector<TInputChunkPtr> chunks;
        for (const auto& dataSlice : stripe->DataSlices) {
            chunks.push_back(dataSlice->GetSingleUnversionedChunk());
        }
        return chunks;
    }

    std::vector<TChunkId> ToChunkIds(const std::vector<TInputChunkPtr>& chunks)
    {
        std::vector<TChunkId> result;
        for (const auto& chunk : chunks) {
            result.push_back(chunk->GetChunkId());
        }
        return result;
    }

    bool Same(const std::vector<TInputChunkPtr>& lhs, const std::vector<TInputChunkPtr>& rhs)
    {
        auto lhsChunkIds = ToChunkIds(lhs);
        auto rhsChunkIds = ToChunkIds(rhs);
        if (Mode_ == EChunkMappingMode::Unordered) {
            std::sort(lhsChunkIds.begin(), lhsChunkIds.end());
            std::sort(rhsChunkIds.begin(), rhsChunkIds.end());
        }
        return lhsChunkIds == rhsChunkIds;
    }

    bool CheckMapping(TChunkStripePtr from, TChunkStripePtr to)
    {
        auto mappedFrom = ChunkMapping_->GetMappedStripe(from);
        return Same(ToChunks(mappedFrom), ToChunks(to));
    }
};

TEST_F(TInputChunkMappingTest, SkippedInputChunk)
{
    InitChunkMapping(EChunkMappingMode::Sorted);

    auto chunkA = CreateChunk();
    auto chunkB = CreateChunk();

    chunkA->LowerLimit() = std::make_unique<TLegacyReadLimit>();
    chunkA->LowerLimit()->SetRowIndex(50);
    chunkB->UpperLimit() = std::make_unique<TLegacyReadLimit>();
    chunkB->UpperLimit()->SetRowIndex(200);

    auto stripeAB = CreateStripe({chunkA, chunkB});
    auto stripeB = CreateStripe({chunkB});
    auto stripeNone = CreateStripe({});

    ChunkMapping_->Add(42, stripeAB);
    EXPECT_TRUE(CheckMapping(stripeAB, stripeAB));
    ChunkMapping_->OnChunkDisappeared(chunkA);
    EXPECT_TRUE(CheckMapping(stripeAB, stripeB));
    ChunkMapping_->OnChunkDisappeared(chunkB);
    EXPECT_TRUE(CheckMapping(stripeAB, stripeNone));
}

TEST_F(TInputChunkMappingTest, RegeneratedIntermediateChunk)
{
    InitChunkMapping(EChunkMappingMode::Sorted);

    auto chunkA1 = CreateChunk();
    auto chunkA2 = CreateChunk();

    chunkA1->LowerLimit() = std::make_unique<TLegacyReadLimit>();
    chunkA1->LowerLimit()->SetRowIndex(50);
    chunkA2->LowerLimit() = std::make_unique<TLegacyReadLimit>();
    chunkA2->LowerLimit()->SetRowIndex(50);

    auto stripeA1 = CreateStripe({chunkA1});
    auto stripeA2 = CreateStripe({chunkA2});

    ChunkMapping_->Add(42, stripeA1);
    EXPECT_TRUE(CheckMapping(stripeA1, stripeA1));
    ChunkMapping_->OnStripeRegenerated(42, stripeA2);
    EXPECT_TRUE(CheckMapping(stripeA1, stripeA2));
}

TEST_F(TInputChunkMappingTest, UnorderedSimple)
{
    InitChunkMapping(EChunkMappingMode::Unordered);

    auto chunkA = CreateChunk();
    auto chunkB = CreateChunk();
    auto chunkC = CreateChunk();
    auto chunkD = CreateChunk();
    auto chunkE = CreateChunk();
    auto chunkF = CreateChunk();

    auto stripeABC = CreateStripe({chunkA, chunkB, chunkC});
    auto stripeCBA = CreateStripe({chunkC, chunkB, chunkA});
    auto stripeACBD = CreateStripe({chunkA, chunkC, chunkB, chunkD});
    auto stripeEF = CreateStripe({chunkE, chunkF});
    auto stripeE = CreateStripe({chunkE});
    auto stripeF = CreateStripe({chunkF});
    auto stripeABCEF = CreateStripe({chunkA, chunkB, chunkC, chunkE, chunkF});
    auto stripeABCDEF = CreateStripe({chunkA, chunkB, chunkC, chunkD, chunkE, chunkF});
    auto stripeABCDF = CreateStripe({chunkA, chunkB, chunkC, chunkD, chunkF});
    auto stripeCBAE = CreateStripe({chunkC, chunkB, chunkA, chunkE});
    auto stripeCBAF = CreateStripe({chunkC, chunkB, chunkA, chunkF});
    auto stripeABCDE = CreateStripe({chunkA, chunkB, chunkC, chunkD, chunkE});

    ChunkMapping_->Add(42, stripeABC);
    EXPECT_TRUE(CheckMapping(stripeABC, stripeABC));
    // In unordered chunk mapping order does not matter (as one could expect).
    EXPECT_TRUE(CheckMapping(stripeCBA, stripeABC));
    ChunkMapping_->Add(23, stripeEF);
    EXPECT_TRUE(CheckMapping(stripeABC, stripeABC));
    EXPECT_TRUE(CheckMapping(stripeCBA, stripeABC));
    EXPECT_TRUE(CheckMapping(stripeEF, stripeEF));
    EXPECT_TRUE(CheckMapping(stripeABCEF, stripeABCEF));
    ChunkMapping_->OnStripeRegenerated(42, stripeACBD);
    EXPECT_TRUE(CheckMapping(stripeABC, stripeACBD));
    EXPECT_TRUE(CheckMapping(stripeCBA, stripeACBD));
    EXPECT_TRUE(CheckMapping(stripeEF, stripeEF));
    EXPECT_TRUE(CheckMapping(stripeABCEF, stripeABCDEF));
    ChunkMapping_->OnChunkDisappeared(chunkF);
    EXPECT_TRUE(CheckMapping(stripeEF, stripeE));
    EXPECT_TRUE(CheckMapping(stripeABCEF, stripeABCDE));
    ChunkMapping_->Reset(42, stripeCBA);
    EXPECT_TRUE(CheckMapping(stripeCBA, stripeCBA));
    EXPECT_TRUE(CheckMapping(stripeE, stripeE));
    ChunkMapping_->OnStripeRegenerated(23, stripeF);
    EXPECT_TRUE(CheckMapping(stripeE, stripeF));
}

TEST_F(TInputChunkMappingTest, SortedValidation)
{
    InitChunkMapping(EChunkMappingMode::Sorted);

    auto chunkA1 = CreateChunk(BuildRow({5}), BuildRow({15}), 1000 /*rowCount*/);
    auto chunkA2 = CreateChunk(BuildRow({5}), BuildRow({15}), 1000 /*rowCount*/); // Compatible.
    auto chunkA3 = CreateChunk(BuildRow({6}), BuildRow({15}), 1000 /*rowCount*/); // Different min key.
    auto chunkA4 = CreateChunk(BuildRow({5}), BuildRow({16}), 1000 /*rowCount*/); // Different max key.

    auto chunkB1 = CreateChunk(BuildRow({10}), BuildRow({20}), 2000 /*rowCount*/);
    auto chunkB2 = CreateChunk(BuildRow({10}), BuildRow({20}), 2000 /*rowCount*/); // Compatible.
    auto chunkB3 = CreateChunk(BuildRow({10}), BuildRow({20}), 2500 /*rowCount*/); // Different row count.

    auto stripeA1B1 = CreateStripe({chunkA1, chunkB1});
    auto stripeA2B2 = CreateStripe({chunkA2, chunkB2});
    auto stripeA3B1 = CreateStripe({chunkA3, chunkB1});
    auto stripeA4B1 = CreateStripe({chunkA4, chunkB1});
    auto stripeA1B3 = CreateStripe({chunkA1, chunkB3});
    auto stripeB1A1 = CreateStripe({chunkB1, chunkA1});
    auto stripeA1B1B1 = CreateStripe({chunkA1, chunkB1, chunkB1});
    auto stripeB1 = CreateStripe({chunkB1});

    ChunkMapping_->Add(42, stripeA1B1);
    ChunkMapping_->OnStripeRegenerated(42, stripeA2B2);
    EXPECT_THROW(ChunkMapping_->OnStripeRegenerated(42, stripeA1B3), std::exception);
    EXPECT_THROW(ChunkMapping_->OnStripeRegenerated(42, stripeA3B1), std::exception);
    EXPECT_THROW(ChunkMapping_->OnStripeRegenerated(42, stripeA4B1), std::exception);
    EXPECT_THROW(ChunkMapping_->OnStripeRegenerated(42, stripeB1A1), std::exception);
    EXPECT_THROW(ChunkMapping_->OnStripeRegenerated(42, stripeA1B1B1), std::exception);
    EXPECT_THROW(ChunkMapping_->OnStripeRegenerated(42, stripeB1), std::exception);
    ChunkMapping_->Reset(42, stripeA1B3);
    EXPECT_THROW(ChunkMapping_->OnStripeRegenerated(42, stripeA2B2), std::exception);
}

TEST_F(TInputChunkMappingTest, TestChunkSliceLimits)
{
    InitChunkMapping(EChunkMappingMode::Sorted);

    auto chunkA = CreateChunk();
    auto chunkB = CreateChunk();

    auto stripeA = CreateStripe({chunkA});
    auto stripeB = CreateStripe({chunkB});

    ChunkMapping_->Add(42, stripeA);

    ChunkMapping_->OnStripeRegenerated(42, stripeB);

    auto stripeAWithLimits = CreateStripe({chunkA});
    TInputSliceLimit lowerLimit;
    lowerLimit.KeyBound = TKeyBound::FromRow(BuildRow({12}), /*isInclusive*/ true, /*isUpper*/ false);
    lowerLimit.RowIndex = 34;
    TInputSliceLimit upperLimit;
    upperLimit.KeyBound = TKeyBound::FromRow(BuildRow({56}), /*isInclusive*/ false, /*isUpper*/ false);
    upperLimit.RowIndex = 78;
    stripeAWithLimits->DataSlices[0]->LowerLimit() = stripeAWithLimits->DataSlices[0]->ChunkSlices[0]->LowerLimit() = lowerLimit;
    stripeAWithLimits->DataSlices[0]->UpperLimit() = stripeAWithLimits->DataSlices[0]->ChunkSlices[0]->UpperLimit() = upperLimit;

    auto mappedStripeAWithLimits = ChunkMapping_->GetMappedStripe(stripeAWithLimits);

    auto oldLowerLimit = mappedStripeAWithLimits->DataSlices[0]->ChunkSlices[0]->LowerLimit();
    auto newLowerLimit = stripeAWithLimits->DataSlices[0]->ChunkSlices[0]->LowerLimit();
    EXPECT_EQ(oldLowerLimit.KeyBound, newLowerLimit.KeyBound);
    EXPECT_EQ(oldLowerLimit.RowIndex, newLowerLimit.RowIndex);

    auto oldUpperLimit = mappedStripeAWithLimits->DataSlices[0]->ChunkSlices[0]->UpperLimit();
    auto newUpperLimit = stripeAWithLimits->DataSlices[0]->ChunkSlices[0]->UpperLimit();
    EXPECT_EQ(oldUpperLimit.KeyBound, newUpperLimit.KeyBound);
    EXPECT_EQ(oldUpperLimit.RowIndex, newUpperLimit.RowIndex);

    EXPECT_EQ(mappedStripeAWithLimits->DataSlices[0]->ChunkSlices[0]->GetInputChunk(), chunkB);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NControllerAgent

#include <yt/yt/ytlib/tablet_client/pivot_keys_builder.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include "yt/yt/ytlib/chunk_client/input_chunk.h"
#include "yt/yt/ytlib/chunk_client/input_chunk_slice.h"

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/yson/string.h>

namespace NYT {
namespace {

using namespace NProto;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////////////////

i64 GetExpectedTabletDataWeight(i64 tabletCount, i64 chunkCount, i64 dataWeight)
{
    return (dataWeight * chunkCount + (tabletCount - 1)) / tabletCount;
}

class TReshardPivotKeysBuilderBase
{
public:
    void SetUpBuilder(
        int tabletCount,
        double accuracy,
        i64 expectedTabletSize)
    {
        Builder_ = std::make_unique<TReshardPivotKeysBuilder>(
            TComparator({ESortOrder::Ascending}),
            /*keyColumnCount*/ 1,
            tabletCount,
            accuracy,
            expectedTabletSize,
            MaxKey());

        TabletCount_ = tabletCount;
        Accuracy_ = accuracy;
        ExpectedTabletSize_ = expectedTabletSize;
    }

    void AddChunk(const TUnversionedOwningRow& min, const TUnversionedOwningRow& max, i64 dataWeight)
    {
        Builder_->AddChunk(CreateChunkSpec(min, max, dataWeight));
    }

    void AddSlice(const TUnversionedOwningRow& lowerLimit, const TUnversionedOwningRow& upperLimit, i64 dataWeight)
    {
        auto chunkSpec = CreateChunkSpec(lowerLimit, upperLimit, dataWeight);
        auto inputChunk = New<TInputChunk>(chunkSpec);

        auto slice = CreateInputChunkSlice(inputChunk, lowerLimit, upperLimit);
        slice->TransformToNew(RowBuffer_, {});
        Builder_->AddSlice(slice);
    }

    void AddChunkPart(const TUnversionedOwningRow& min, const TUnversionedOwningRow& max, i64 dataWeight)
    {
        auto chunkSpec = CreateChunkSpec(min, max, dataWeight);

        NChunkClient::TReadLimit lowerLimit(TOwningKeyBound::FromRow() >= min);
        NChunkClient::TReadLimit upperLimit(TOwningKeyBound::FromRow() < max);

        ToProto(chunkSpec.mutable_lower_limit(), lowerLimit);
        ToProto(chunkSpec.mutable_lower_limit(), upperLimit);

        auto inputChunk = New<TInputChunk>(chunkSpec);
        auto weightedChunk = New<TWeightedInputChunk>(inputChunk, dataWeight);
        Builder_->AddChunk(weightedChunk);
    }

    TReshardPivotKeysBuilder* GetBuilder()
    {
        return Builder_.get();
    }

    void CheckPivotKeys(const std::vector<TUnversionedOwningRow>& expectedPivotKeys) const
    {
        ASSERT_TRUE(Builder_->AreAllPivotsFound());

        auto pivotKeys = Builder_->GetPivotKeys();
        ASSERT_EQ(std::ssize(pivotKeys), TabletCount_);

        ASSERT_EQ(pivotKeys[0], EmptyKey());
        for (int index = 1; index < TabletCount_; ++index) {
            ASSERT_EQ(pivotKeys[index], expectedPivotKeys[index - 1]);
        }
    }

private:
    std::unique_ptr<TReshardPivotKeysBuilder> Builder_;
    i64 TabletCount_;
    double Accuracy_;
    i64 ExpectedTabletSize_;

    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>();

    NChunkClient::NProto::TChunkSpec CreateChunkSpec(
        const TUnversionedOwningRow& min,
        const TUnversionedOwningRow& max,
        i64 dataWeight)
    {
        TChunkSpec chunkSpec;

        chunkSpec.set_table_index(0);
        chunkSpec.set_range_index(0);

        NChunkClient::NProto::TChunkMeta chunkMeta;
        TMiscExt miscExt;
        miscExt.set_data_weight(dataWeight);
        miscExt.set_row_count(2);

        TBoundaryKeysExt boundaryKeys;
        ToProto(boundaryKeys.mutable_min(), min);
        ToProto(boundaryKeys.mutable_max(), max);

        SetProtoExtension(chunkMeta.mutable_extensions(), miscExt);
        SetProtoExtension(chunkMeta.mutable_extensions(), boundaryKeys);

        chunkMeta.set_type(static_cast<int>(EChunkType::Table));

        ToProto(chunkSpec.mutable_chunk_meta(), chunkMeta);

        return chunkSpec;
    }
};

class TReshardPivotKeysBuilderTest
    : public ::testing::Test
    , public TReshardPivotKeysBuilderBase
{ };

// Chunk layout:          --- --- --- ---
// Tablet pivot keys:    |-------|-------|
TEST_F(TReshardPivotKeysBuilderTest, ComputeChunksForSlicingEmpty)
{
    i64 tabletCount = 2;

    SetUpBuilder(tabletCount, /*accuracy*/ 0.05, /*expectedTabletSize*/ 200);

    AddChunk(YsonToKey("1u"), YsonToKey("2u"), 100);
    AddChunk(YsonToKey("3u"), YsonToKey("4u"), 100);
    AddChunk(YsonToKey("5u"), YsonToKey("6u"), 100);
    AddChunk(YsonToKey("7u"), YsonToKey("8u"), 100);

    GetBuilder()->SetFirstPivotKey(EmptyKey());
    GetBuilder()->ComputeChunksForSlicing();

    CheckPivotKeys({YsonToKey("5u")});
}

// Chunk layout:         -- --
//                       -- --
//                       -- --
// Tablet pivot keys:    |-|-|
TEST_F(TReshardPivotKeysBuilderTest, ComputeChunksForSlicingWithWholeOverlapping)
{
    i64 tabletCount = 2;
    i64 chunkCount = 5;
    i64 dataWeight = 100;

    SetUpBuilder(
        tabletCount,
        /*accuracy*/ 0.05,
        /*expectedTabletSize*/ dataWeight * chunkCount);

    for (int index = 0; index < 5; ++index) {
        AddChunk(YsonToKey("1u"), YsonToKey("4u"), dataWeight);
        AddChunk(YsonToKey("5u"), YsonToKey("8u"), dataWeight);
    }

    GetBuilder()->SetFirstPivotKey(EmptyKey());
    GetBuilder()->ComputeChunksForSlicing();

    CheckPivotKeys({YsonToKey("5u")});
}

// Chunk layout:          --- --- ---- ----
// Tablet pivot keys:    |--|--|--|--|--|--|
TEST_F(TReshardPivotKeysBuilderTest, TooSmallComputeChunksForSlicing)
{
    i64 tabletCount = 6;
    i64 chunkCount = 4;
    i64 dataWeight = 100;

    SetUpBuilder(
        tabletCount,
        /*accuracy*/ 0.05,
        /*expectedTabletSize*/ dataWeight * chunkCount);

    for (int index = 0; index < chunkCount; ++index) {
        AddSlice(YsonToKey(ToString(index)), YsonToKey(ToString(index + 1)), 2);
    }

    GetBuilder()->SetFirstPivotKey(EmptyKey());
    GetBuilder()->ComputeChunksForSlicing();

    ASSERT_FALSE(GetBuilder()->AreAllPivotsFound());
}

////////////////////////////////////////////////////////////////////////////////

// Chunk layout:          ---- -- -- ----
// Tablet pivot keys:    |-------|-------|
TEST_F(TReshardPivotKeysBuilderTest, ComputeSlicedChunksPivotKeys)
{
    i64 tabletCount = 2;
    i64 dataWeight = 100;

    SetUpBuilder(tabletCount, /*accuracy*/ 0.05, /*expectedTabletSize*/ 150);

    AddChunk(YsonToKey("1u"), YsonToKey("2u"), dataWeight);
    AddChunk(YsonToKey("7u"), YsonToKey("8u"), dataWeight);

    AddSlice(YsonToKey("3u"), YsonToKey("4u"), dataWeight / 2);
    AddSlice(YsonToKey("5u"), YsonToKey("6u"), dataWeight / 2);

    GetBuilder()->SetFirstPivotKey(EmptyKey());
    GetBuilder()->ComputeSlicedChunksPivotKeys();

    CheckPivotKeys({YsonToKey("5u")});
}

// Chunk layout:          --- --- --- --- ---
// Tablet pivot keys:    |---|---|---|---|---|
TEST_F(TReshardPivotKeysBuilderTest, ComputePivotKeysOneChunkToTablet)
{
    i64 chunkCount = 5;
    i64 dataWeight = 100;

    SetUpBuilder(chunkCount, /*accuracy*/ 0.05, /*expectedTabletSize*/ dataWeight);

    for (int index = 0; index < chunkCount; ++index) {
        i64 firstKey = index * 2;
        AddChunk(YsonToKey(ToString(firstKey)), YsonToKey(ToString(firstKey + 1)), dataWeight);
    }

    GetBuilder()->SetFirstPivotKey(EmptyKey());
    GetBuilder()->ComputeSlicedChunksPivotKeys();

    CheckPivotKeys({YsonToKey("2"), YsonToKey("4"), YsonToKey("6"), YsonToKey("8")});
}

// Chunk layout:          --- --- ---- ----
// Tablet pivot keys:    |--|--|--|--|--|--|
TEST_F(TReshardPivotKeysBuilderTest, TooSmallComputePivotKeys)
{
    i64 tabletCount = 6;
    i64 chunkCount = 4;
    i64 dataWeight = 100;

    SetUpBuilder(
        tabletCount,
        /*accuracy*/ 0.05,
        GetExpectedTabletDataWeight(tabletCount, chunkCount, dataWeight));

    for (int index = 0; index < chunkCount; ++index) {
        AddChunk(YsonToKey(ToString(index)), YsonToKey(ToString(index + 1)), dataWeight);
    }

    GetBuilder()->SetFirstPivotKey(EmptyKey());
    GetBuilder()->ComputeSlicedChunksPivotKeys();

    ASSERT_FALSE(GetBuilder()->AreAllPivotsFound());
}

// Chunk layout:          ------ -- --- --
//                                --- ---
// Tablet pivot keys:    |-------|-------|
TEST_F(TReshardPivotKeysBuilderTest, ComputePivotKeysFairBetterThanBrute)
{
    i64 tabletCount = 2;
    SetUpBuilder(tabletCount, /*accuracy*/ 0.05, /*expectedTabletSize*/ 100);

    AddChunk(YsonToKey("1"), YsonToKey("2"), 95);

    for (int index = 3; index < 24; ++index) {
        AddChunk(YsonToKey(ToString(index)), YsonToKey(ToString(index + 1)), 5);
    }

    GetBuilder()->SetFirstPivotKey(EmptyKey());
    GetBuilder()->ComputeSlicedChunksPivotKeys();

    CheckPivotKeys({YsonToKey(ToString(3))});
}

// Chunk layout:          ---- -.- -.- ----
// Tablet pivot keys:    |--------|--------|
TEST_F(TReshardPivotKeysBuilderTest, ComputeSlicedChunksBestPivotKeys)
{
    i64 tabletCount = 2;
    i64 dataWeight = 100;
    i64 smallChunkCount = 100;

    SetUpBuilder(tabletCount, /*accuracy*/ 0.05, /*expectedTabletSize*/ dataWeight);

    for (int index = 0; index < smallChunkCount; ++index) {
        i64 firstKey = index * 2;
        AddSlice(YsonToKey(ToString(firstKey)), YsonToKey(ToString(firstKey + 1)), 2);
    }

    GetBuilder()->SetFirstPivotKey(EmptyKey());
    GetBuilder()->ComputeSlicedChunksPivotKeys();

    CheckPivotKeys({YsonToKey(ToString(dataWeight))});
}

// Chunk layout:          -- -- -- -- --
// Tablet pivot keys:    |-------|------|
TEST_F(TReshardPivotKeysBuilderTest, ComputePivotKeysBrute)
{
    i64 tabletCount = 2;
    i64 dataWeight = 100;

    SetUpBuilder(
        tabletCount,
        /*accuracy*/ 0.05,
        GetExpectedTabletDataWeight(tabletCount, /*chunkCount*/ 5, dataWeight));

    AddChunk(YsonToKey("1u"), YsonToKey("2u"), dataWeight);
    AddChunk(YsonToKey("7u"), YsonToKey("8u"), dataWeight);
    AddChunk(YsonToKey("9u"), YsonToKey("10u"), dataWeight);

    AddSlice(YsonToKey("3u"), YsonToKey("4u"), dataWeight);
    AddSlice(YsonToKey("5u"), YsonToKey("6u"), dataWeight);

    GetBuilder()->SetFirstPivotKey(EmptyKey());
    GetBuilder()->ComputeSlicedChunksPivotKeys();

    CheckPivotKeys({YsonToKey("6u")});
}

// Chunk layout:          --- --- --- --- --- --- ---
//                          --- --- --- --- --- ---
// Tablet pivot keys:    |--------|---------|--------|
TEST_F(TReshardPivotKeysBuilderTest, ComputePivotKeysBruteWithOverlapping)
{
    i64 tabletCount = 3;
    i64 chunkCount = 111;
    i64 dataWeight = 100;

    SetUpBuilder(
        tabletCount,
        /*accuracy*/ 0.05,
        GetExpectedTabletDataWeight(tabletCount, chunkCount, dataWeight));

    for (int index = 0; index < chunkCount; ++index) {
        i64 firstKey = index * 2;
        AddChunk(YsonToKey(ToString(firstKey)), YsonToKey(ToString(firstKey + 3)), dataWeight);
    }

    GetBuilder()->SetFirstPivotKey(EmptyKey());
    GetBuilder()->ComputeSlicedChunksPivotKeys();

    CheckPivotKeys({YsonToKey("76"), YsonToKey("150")});
}

////////////////////////////////////////////////////////////////////////////////

class TComputeChunksForSlicingTest
    : public TReshardPivotKeysBuilderBase
    , public ::testing::TestWithParam<std::tuple<i64, i64, i64, bool>>
{
public:
    static constexpr i64 DataWeight = 100;

    void SetUp() override
    {
        i64 tabletCount = std::get<0>(GetParam());
        i64 chunkCount = std::get<1>(GetParam());
        bool overlapping = std::get<3>(GetParam());

        SetUpBuilder(
            tabletCount,
            0.05,
            GetExpectedTabletDataWeight(tabletCount, chunkCount, DataWeight));

        for (int index = 0; index < chunkCount; ++index) {
            i64 firstKey = index * 2;
            if (overlapping) {
                AddChunk(YsonToKey(ToString(firstKey)), YsonToKey(ToString(firstKey + 3)), DataWeight);
            } else {
                AddChunk(YsonToKey(ToString(firstKey)), YsonToKey(ToString(firstKey + 1)), DataWeight);
            }
        }
    }
};

TEST_P(TComputeChunksForSlicingTest, ComputeChunksForSlicing)
{
    i64 forSlicingCount = std::get<2>(GetParam());
    GetBuilder()->ComputeChunksForSlicing();

    ASSERT_EQ(std::ssize(GetBuilder()->GetChunksForSlicing()), forSlicingCount);

    if (forSlicingCount == 1) {
        const auto [inputChunk, realDataWeight] = *GetBuilder()->GetChunksForSlicing().begin();

        ASSERT_EQ(DataWeight, realDataWeight);

        i64 tabletCount = std::get<0>(GetParam());
        i64 chunkCount = std::get<1>(GetParam());
        i64 minKey = (chunkCount / tabletCount) * 2;

        ASSERT_EQ(inputChunk->BoundaryKeys()->MinKey, YsonToKey(ToString(minKey)));
        ASSERT_EQ(inputChunk->BoundaryKeys()->MaxKey, YsonToKey(ToString(minKey + 1)));
    }
}

INSTANTIATE_TEST_SUITE_P(Test,
    TComputeChunksForSlicingTest,
    ::testing::Values(
        // Chunk layout:          ---
        // Tablet pivot keys:    |---|
        std::tuple(/*tabletCount*/ 1, /*chunkCount*/ 1, /*forSlicingCount*/ 0, /*overlapping*/ false),
        // Chunk layout:          --- --- ---
        // Tablet pivot keys:    |-----------|
        std::tuple(/*tabletCount*/ 1, /*chunkCount*/ 3, /*forSlicingCount*/ 0, /*overlapping*/ false),
        // Chunk layout:          --- --- --- ---
        // Tablet pivot keys:    |-------|-------|
        std::tuple(/*tabletCount*/ 4, /*chunkCount*/ 2, /*forSlicingCount*/ 2, /*overlapping*/ false),
        // Chunk layout:          --- --- --- --- --- ---
        // Tablet pivot keys:    |-------|-------|-------|
        std::tuple(/*tabletCount*/ 6, /*chunkCount*/ 3, /*forSlicingCount*/ 3, /*overlapping*/ false),
        // Chunk layout:          -- -- -- -- -- -- -- -- --
        // Tablet pivot keys:    |--------|--------|--------|
        std::tuple(/*tabletCount*/ 9, /*chunkCount*/ 3, /*forSlicingCount*/ 3, /*overlapping*/ false),
        // Chunk layout:          --- --- ---
        // Tablet pivot keys:    |-----|-----|
        std::tuple(/*tabletCount*/ 2, /*chunkCount*/ 3, /*forSlicingCount*/ 1, /*overlapping*/ false),
        // Chunk layout:         --- --- --- --- ---
        // Tablet pivot keys:    |-----|-----|-----|
        std::tuple(/*tabletCount*/ 3, /*chunkCount*/ 5, /*forSlicingCount*/ 2, /*overlapping*/ false),
        // Chunk layout:         -------------
        // Tablet pivot keys:    |---|---|---|
        std::tuple(/*tabletCount*/ 3, /*chunkCount*/ 1, /*forSlicingCount*/ 1, /*overlapping*/ false),
        // Chunk layout:         --- --- --- --- --- --- ---
        //                         --- --- --- --- --- ---
        // Tablet pivot keys:    |-------------------------|
        std::tuple(/*tabletCount*/ 1, /*chunkCount*/ 11, /*forSlicingCount*/ 0, /*overlapping*/ true),
        // Chunk layout:         --- --- --- --- --- --- ---
        //                         --- --- --- --- --- ---
        // Tablet pivot keys:    |--------|--------|--------|
        std::tuple(/*tabletCount*/ 3, /*chunkCount*/ 11, /*forSlicingCount*/ 5, /*overlapping*/ true))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

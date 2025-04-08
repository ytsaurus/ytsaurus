#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NChunkClient {
namespace {

using namespace testing;

using namespace NChunkClient;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

using NChunkClient::NProto::TChunkSpec;
using NChunkClient::NProto::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

class TAbsoluteLimitsTest
    : public Test
{
protected:
    static TChunkSpec CreateChunk(
        std::optional<i64> tableRowIndex = std::nullopt,
        std::optional<i64> rowCountOverride = std::nullopt,
        std::optional<int> tabletIndex = std::nullopt,
        std::optional<TReadLimit> lowerLimit = std::nullopt,
        std::optional<TReadLimit> upperLimit = std::nullopt)
    {
        TChunkSpec spec;

        ToProto(spec.mutable_chunk_id(), MakeRandomId(EObjectType::Chunk, TCellTag(0x42)));

        if (tableRowIndex.has_value()) {
            spec.set_table_row_index(*tableRowIndex);
        }
        if (rowCountOverride.has_value()) {
            spec.set_row_count_override(*rowCountOverride);
        }
        if (tabletIndex.has_value()) {
            spec.set_tablet_index(*tabletIndex);
        }
        if (lowerLimit.has_value()) {
            *spec.mutable_lower_limit() = *lowerLimit;
        }
        if (upperLimit.has_value()) {
            *spec.mutable_upper_limit() = *upperLimit;
        }

        return spec;
    }

    static TReadLimit CreateReadLimit(
        std::optional<i64> rowIndex = std::nullopt,
        std::optional<TLegacyOwningKey> legacyKey = std::nullopt)
    {
        TReadLimit result;
        if (rowIndex.has_value()) {
            result.set_row_index(*rowIndex);
        }
        if (legacyKey.has_value()) {
            ToProto(result.mutable_legacy_key(), *legacyKey);
        }
        return result;
    }

    static void CheckNonVersionedLimit(const TLegacyReadLimit& limit)
    {
        ASSERT_TRUE(limit.HasRowIndex());
        ASSERT_FALSE(limit.HasOffset());
        ASSERT_FALSE(limit.HasChunkIndex());
        ASSERT_FALSE(limit.HasTabletIndex());
    }

    static void CheckVersionedSortedLimit(const TLegacyReadLimit& limit)
    {
        ASSERT_FALSE(limit.HasRowIndex());
        ASSERT_FALSE(limit.HasOffset());
        ASSERT_FALSE(limit.HasChunkIndex());
        ASSERT_FALSE(limit.HasTabletIndex());
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TAbsoluteLimitsTest, EmptyChunkNoLimits)
{
    TDataSliceDescriptor descriptor(CreateChunk());

    auto lowerLimit = GetAbsoluteLowerReadLimit(descriptor, /*versioned*/ false, /*sorted*/ false);
    CheckNonVersionedLimit(lowerLimit);
    EXPECT_EQ(lowerLimit.GetRowIndex(), 0);

    auto upperLimit = GetAbsoluteUpperReadLimit(descriptor, /*versioned*/ false, /*sorted*/ false);
    CheckNonVersionedLimit(upperLimit);
    EXPECT_EQ(upperLimit.GetRowIndex(), 0);
}

TEST_F(TAbsoluteLimitsTest, ChunkWithRowsNoLimits)
{
    TDataSliceDescriptor descriptor(CreateChunk(/*tableRowIndex*/ std::nullopt, /*rowCountOverride*/ 42));

    auto lowerLimit = GetAbsoluteLowerReadLimit(descriptor, /*versioned*/ false, /*sorted*/ false);
    CheckNonVersionedLimit(lowerLimit);
    EXPECT_EQ(lowerLimit.GetRowIndex(), 0);

    auto upperLimit = GetAbsoluteUpperReadLimit(descriptor, /*versioned*/ false, /*sorted*/ false);
    CheckNonVersionedLimit(upperLimit);
    EXPECT_EQ(upperLimit.GetRowIndex(), 42);
}

TEST_F(TAbsoluteLimitsTest, ChunkWithRowsAndLimits1)
{
    TDataSliceDescriptor descriptor(CreateChunk(
        /*tableRowIndex*/ std::nullopt,
        /*rowCountOverride*/ 42,
        /*tabletIndex*/ std::nullopt,
        /*lowerLimit*/ CreateReadLimit(/*rowIndex*/ 10),
        /*upperLimit*/ CreateReadLimit(/*rowIndex*/ 30)));

    auto lowerLimit = GetAbsoluteLowerReadLimit(descriptor, /*versioned*/ false, /*sorted*/ false);
    CheckNonVersionedLimit(lowerLimit);
    EXPECT_EQ(lowerLimit.GetRowIndex(), 10);

    auto upperLimit = GetAbsoluteUpperReadLimit(descriptor, /*versioned*/ false, /*sorted*/ false);
    CheckNonVersionedLimit(upperLimit);
    EXPECT_EQ(upperLimit.GetRowIndex(), 30);
}

TEST_F(TAbsoluteLimitsTest, ChunkWithRowsAndLimits2)
{
    TDataSliceDescriptor descriptor(CreateChunk(
        /*tableRowIndex*/ std::nullopt,
        /*rowCountOverride*/ 42,
        /*tabletIndex*/ std::nullopt,
        /*lowerLimit*/ CreateReadLimit(/*rowIndex*/ 50),
        /*upperLimit*/ CreateReadLimit(/*rowIndex*/ 70)));

    auto lowerLimit = GetAbsoluteLowerReadLimit(descriptor, /*versioned*/ false, /*sorted*/ false);
    CheckNonVersionedLimit(lowerLimit);
    EXPECT_EQ(lowerLimit.GetRowIndex(), 50);

    auto upperLimit = GetAbsoluteUpperReadLimit(descriptor, /*versioned*/ false, /*sorted*/ false);
    CheckNonVersionedLimit(upperLimit);
    EXPECT_EQ(upperLimit.GetRowIndex(), 70);
}

TEST_F(TAbsoluteLimitsTest, ChunkWithRowsAndLimits3)
{
    TDataSliceDescriptor descriptor(CreateChunk(
        /*tableRowIndex*/ std::nullopt,
        /*rowCountOverride*/ 42,
        /*tabletIndex*/ std::nullopt,
        /*lowerLimit*/ CreateReadLimit(),
        /*upperLimit*/ CreateReadLimit(/*rowIndex*/ 50)));

    auto lowerLimit = GetAbsoluteLowerReadLimit(descriptor, /*versioned*/ false, /*sorted*/ false);
    CheckNonVersionedLimit(lowerLimit);
    EXPECT_EQ(lowerLimit.GetRowIndex(), 0);

    auto upperLimit = GetAbsoluteUpperReadLimit(descriptor, /*versioned*/ false, /*sorted*/ false);
    CheckNonVersionedLimit(upperLimit);
    EXPECT_EQ(upperLimit.GetRowIndex(), 50);
}

TEST_F(TAbsoluteLimitsTest, ChunkWithRowsAndRowIndexNoLimits)
{
    TDataSliceDescriptor descriptor(CreateChunk(
        /*tableRowIndex*/ 50,
        /*rowCountOverride*/ 42));

    auto lowerLimit = GetAbsoluteLowerReadLimit(descriptor, /*versioned*/ false, /*sorted*/ false);
    CheckNonVersionedLimit(lowerLimit);
    EXPECT_EQ(lowerLimit.GetRowIndex(), 50);

    auto upperLimit = GetAbsoluteUpperReadLimit(descriptor, /*versioned*/ false, /*sorted*/ false);
    CheckNonVersionedLimit(upperLimit);
    EXPECT_EQ(upperLimit.GetRowIndex(), 92);
}

TEST_F(TAbsoluteLimitsTest, VersionedAndSorted)
{
    auto buildRow = [] (i64 value) {
        TUnversionedOwningRowBuilder rowBuilder;
        rowBuilder.AddValue(MakeUnversionedInt64Value(value));
        return rowBuilder.FinishRow();
    };

    TDataSliceDescriptor descriptor({
        CreateChunk(
            /*tableRowIndex*/ std::nullopt,
            /*rowCountOverride*/ std::nullopt,
            /*tabletIndex*/ std::nullopt,
            /*lowerLimit*/ CreateReadLimit(/*rowIndex*/ std::nullopt, /*legacyKey*/ buildRow(1)),
            /*upperLimit*/ CreateReadLimit(/*rowIndex*/ std::nullopt, /*legacyKey*/ buildRow(2))),
        CreateChunk(
            /*tableRowIndex*/ std::nullopt,
            /*rowCountOverride*/ std::nullopt,
            /*tabletIndex*/ std::nullopt,
            /*lowerLimit*/ CreateReadLimit(/*rowIndex*/ std::nullopt, /*legacyKey*/ buildRow(0)),
            /*upperLimit*/ CreateReadLimit(/*rowIndex*/ std::nullopt, /*legacyKey*/ buildRow(42))),
    });

    auto lowerLimit = GetAbsoluteLowerReadLimit(descriptor, /*versioned*/ true, /*sorted*/ true);
    CheckVersionedSortedLimit(lowerLimit);
    EXPECT_EQ(lowerLimit.GetLegacyKey().Begin()->Data.Int64, 0);

    auto upperLimit = GetAbsoluteUpperReadLimit(descriptor, /*versioned*/ true, /*sorted*/ true);
    CheckVersionedSortedLimit(upperLimit);
    EXPECT_EQ(upperLimit.GetLegacyKey().Begin()->Data.Int64, 42);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkClient

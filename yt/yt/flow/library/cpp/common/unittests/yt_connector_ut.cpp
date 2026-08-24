#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/yt_connector.h>

namespace NYT::NFlow {
namespace {

using namespace NDetail;
using NObjectClient::EObjectType;

////////////////////////////////////////////////////////////////////////////////

TInternalTableInfo MakePlainTable(std::string tabletCellBundle)
{
    return TInternalTableInfo{
        .Type = EObjectType::Table,
        .TabletCellBundle = std::move(tabletCellBundle),
    };
}

TInternalTableInfo MakeChaosTable(std::string tabletCellBundle)
{
    return TInternalTableInfo{
        .Type = EObjectType::ChaosReplicatedTable,
        .TabletCellBundle = std::move(tabletCellBundle),
    };
}

////////////////////////////////////////////////////////////////////////////////

TEST(TEnsureSameTabletCellBundleTest, OneBundlePasses)
{
    EXPECT_NO_THROW(EnsureSameTabletCellBundle({
        MakePlainTable("bigb"),
        MakeChaosTable("bigb"),
    }));
}

TEST(TEnsureSameTabletCellBundleTest, DivergingBundlesThrow)
{
    try {
        EnsureSameTabletCellBundle({
            MakePlainTable("bigb"),
            MakePlainTable("bigb-prestable"),
        });
        ADD_FAILURE() << "Expected different tablet cell bundles to be rejected";
    } catch (const TErrorException& ex) {
        EXPECT_THAT(ex.Error().GetMessage(), ::testing::HasSubstr("must be in the same bundle"));
        EXPECT_TRUE(ex.Error().Attributes().Contains("bundle_names"));
    }
}

TEST(TEnsureSameTabletCellBundleTest, NoTablesThrow)
{
    EXPECT_THROW_WITH_SUBSTRING(
        EnsureSameTabletCellBundle({}),
        "No internal flow tables found to determine the bundle");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TEnsureSameTableTypeTest, OneTypePasses)
{
    EXPECT_NO_THROW(EnsureSameTableType({
        MakeChaosTable("bigb"),
        MakeChaosTable("bigb"),
    }));
}

TEST(TEnsureSameTableTypeTest, MixedTypesThrow)
{
    try {
        EnsureSameTableType({
            MakeChaosTable("bigb"),
            MakePlainTable("bigb"),
        });
        ADD_FAILURE() << "Expected different table types to be rejected";
    } catch (const TErrorException& ex) {
        EXPECT_THAT(ex.Error().GetMessage(), ::testing::HasSubstr("must be the same type"));
        EXPECT_TRUE(ex.Error().Attributes().Contains("table_types"));
    }
}

TEST(TEnsureSameTableTypeTest, NoTablesThrow)
{
    EXPECT_THROW_WITH_SUBSTRING(
        EnsureSameTableType({}),
        "No internal flow tables found to determine the type");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TIsChaosTableLayoutTest, ChaosTablesAreChaosLayout)
{
    EXPECT_TRUE(IsChaosTableLayout({
        MakeChaosTable("bigb"),
        MakeChaosTable("bigb"),
    }));
}

TEST(TIsChaosTableLayoutTest, PlainTablesAreNotChaosLayout)
{
    EXPECT_FALSE(IsChaosTableLayout({
        MakePlainTable("bigb"),
        MakePlainTable("bigb"),
    }));
}

TEST(TIsChaosTableLayoutTest, MixedTypesThrow)
{
    EXPECT_THROW_WITH_SUBSTRING(
        IsChaosTableLayout({
            MakeChaosTable("bigb"),
            MakePlainTable("bigb"),
        }),
        "must be the same type");
}

TEST(TIsChaosTableLayoutTest, NoTablesThrow)
{
    EXPECT_THROW_WITH_SUBSTRING(
        IsChaosTableLayout({}),
        "No internal flow tables found to determine the type");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow

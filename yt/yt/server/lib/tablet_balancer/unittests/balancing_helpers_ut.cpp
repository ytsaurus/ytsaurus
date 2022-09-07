#include <yt/yt/server/lib/tablet_balancer/balancing_helpers.h>
#include <yt/yt/server/lib/tablet_balancer/config.h>
#include <yt/yt/server/lib/tablet_balancer/table.h>
#include <yt/yt/server/lib/tablet_balancer/tablet.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell_bundle.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NTabletBalancer {
namespace {

using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TObjectId MakeSequentialId(EObjectType type)
{
    static int counter = 0;
    return MakeId(type, /*cellTag*/ MinValidCellTag, /*counter*/ 0, /*hash*/ counter++);
}

struct TTestTablet
    : public TYsonSerializable
{
    i64 UncompressedDataSize;
    i64 MemorySize;
    int CellIndex;

    TTestTablet()
    {
        RegisterParameter("uncompressed_data_size", UncompressedDataSize)
            .Default(100);
        RegisterParameter("memory_size", MemorySize)
            .Default(100);
        RegisterParameter("cell_index", CellIndex)
            .Default(0);
    }

    TTabletPtr CreateTablet(
        const TTabletId& tabletId,
        const TTablePtr& table,
        const std::vector<TTabletCellPtr>& cells) const
    {
        auto tablet = New<TTablet>(
            tabletId,
            table.Get());

        tablet->Statistics.UncompressedDataSize = UncompressedDataSize;
        tablet->Statistics.MemorySize = MemorySize;
        tablet->Cell = cells[CellIndex].Get();
        tablet->Cell->Tablets.push_back(tablet);

        return tablet;
    }
};

DECLARE_REFCOUNTED_STRUCT(TTestTablet)
DEFINE_REFCOUNTED_TYPE(TTestTablet)

////////////////////////////////////////////////////////////////////////////////

struct TTestTable
    : public TYsonSerializable
{
    bool Sorted;
    i64 CompressedDataSize;
    i64 UncompressedDataSize;
    i64 DataWeight;
    EInMemoryMode InMemoryMode;

    TTestTable()
    {
        RegisterParameter("sorted", Sorted)
            .Default(true);
        RegisterParameter("compressed_data_size", CompressedDataSize)
            .Default(100);
        RegisterParameter("uncompressed_data_size", UncompressedDataSize)
            .Default(100);
        RegisterParameter("data_weight", DataWeight)
            .Default(100);
        RegisterParameter("in_memory_mode", InMemoryMode)
            .Default(EInMemoryMode::None);
    }

    TTablePtr CreateTable(const TTabletCellBundlePtr& bundle) const
    {
        auto tableId = MakeSequentialId(EObjectType::Table);
        auto table = New<TTable>(
            Sorted,
            ToString(tableId),
            MinValidCellTag,
            tableId,
            bundle.Get());

        table->Dynamic = true;
        table->CompressedDataSize = CompressedDataSize;
        table->UncompressedDataSize = UncompressedDataSize;
        table->DataWeight = DataWeight;
        table->InMemoryMode = InMemoryMode;
        return table;
    }
};

DECLARE_REFCOUNTED_STRUCT(TTestTable)
DEFINE_REFCOUNTED_TYPE(TTestTable)

////////////////////////////////////////////////////////////////////////////////

struct TTestTabletCell
    : public TYsonSerializable
{
    i64 MemorySize;

    TTestTabletCell()
    {
        RegisterParameter("memory_size", MemorySize)
            .Default(100);
    }
};

DECLARE_REFCOUNTED_STRUCT(TTestTabletCell)
DEFINE_REFCOUNTED_TYPE(TTestTabletCell)

////////////////////////////////////////////////////////////////////////////////

struct TTestBundle
{
    TTabletCellBundlePtr Bundle;
    std::vector<TTabletCellPtr> Cells;
    std::vector<TTableId> TableIds;
};

////////////////////////////////////////////////////////////////////////////////

TTestBundle CreateTabletCellBundle(
    TStringBuf cellsString,
    TStringBuf bundleConfig,
    const std::vector<std::tuple<TStringBuf, TStringBuf, std::vector<TStringBuf>>>& tables)
{
    TTestBundle testBundle;
    testBundle.Bundle = New<TTabletCellBundle>("bundle");
    testBundle.Bundle->Config = ConvertTo<TBundleTabletBalancerConfigPtr>(TYsonStringBuf(bundleConfig));

    std::vector<TTabletCellId> cellIds;
    auto cells = ConvertTo<std::vector<TTestTabletCellPtr>>(TYsonStringBuf(cellsString));
    for (int index = 0; index < std::ssize(cells); ++index) {
        cellIds.emplace_back(MakeSequentialId(EObjectType::TabletCell));
    }

    for (int index = 0; index < std::ssize(cells); ++index) {
        auto cell = New<TTabletCell>(
            cellIds[index],
            TTabletCellStatistics{.MemorySize = cells[index]->MemorySize},
            TTabletCellStatus{.Health = NTabletClient::ETabletCellHealth::Good, .Decommissioned = false});
        testBundle.Bundle->TabletCells.emplace(cellIds[index], cell);
        testBundle.Cells.push_back(cell);
    }

    for (const auto& [tableSettings, tableConfig, tablets] : tables) {
        auto table = ConvertTo<TTestTablePtr>(TYsonStringBuf(tableSettings))->CreateTable(testBundle.Bundle);

        table->Bundle = testBundle.Bundle.Get();
        table->TableConfig = ConvertTo<TTableTabletBalancerConfigPtr>(TYsonStringBuf(tableConfig));

        std::vector<TTabletId> tabletIds;
        for (int index = 0; index < std::ssize(tablets); ++index) {
            tabletIds.emplace_back(MakeSequentialId(EObjectType::Tablet));
        }

        for (int index = 0; index < std::ssize(tablets); ++index) {
            auto tablet = ConvertTo<TTestTabletPtr>(TYsonStringBuf(tablets[index]))->CreateTablet(
                tabletIds[index],
                table,
                testBundle.Cells);
            tablet->Index = std::ssize(table->Tablets);
            table->Tablets.push_back(std::move(tablet));
        }

        EmplaceOrCrash(testBundle.Bundle->Tables, table->Id, table);
        testBundle.TableIds.push_back(table->Id);
    }

    for (const auto& [id, cell] : testBundle.Bundle->TabletCells) {
        i64 tabletsSize = 0;
        for (const auto& tablet : cell->Tablets) {
            tabletsSize += GetTabletBalancingSize(tablet);
        }

        EXPECT_LE(tabletsSize, cell->Statistics.MemorySize);
    }

    return testBundle;
}

////////////////////////////////////////////////////////////////////////////////

struct TTestMoveDescriptor
    : public TYsonSerializable
{
    int TableIndex;
    int TabletIndex;
    int CellIndex;

    TTestMoveDescriptor()
    {
        RegisterParameter("table_index", TableIndex)
            .Default(0);
        RegisterParameter("tablet_index", TabletIndex)
            .Default(0);
        RegisterParameter("cell_index", CellIndex)
            .Default(0);
    }
};

DECLARE_REFCOUNTED_STRUCT(TTestMoveDescriptor)
DEFINE_REFCOUNTED_TYPE(TTestMoveDescriptor)

bool IsEqual(const TTestMoveDescriptorPtr& expected, const TMoveDescriptor& actual, const TTestBundle& bundle)
{
    const auto& cell = bundle.Cells[expected->CellIndex];
    const auto& table = bundle.TableIds[expected->TableIndex];
    const auto& tablet = GetOrCrash(bundle.Bundle->Tables, table)->Tablets[expected->TabletIndex];
    EXPECT_EQ(actual.TabletId, tablet->Id);
    EXPECT_EQ(actual.TabletCellId, cell->Id);
    return actual.TabletId == tablet->Id && actual.TabletCellId == cell->Id;
}

////////////////////////////////////////////////////////////////////////////////

struct TTestReshardDescriptor
    : public TYsonSerializable
{
    int TableIndex;
    std::vector<int> TabletIndexes;
    i64 DataSize;
    int TabletCount;

    TTestReshardDescriptor()
    {
        RegisterParameter("table_index", TableIndex)
            .Default(0);
        RegisterParameter("tablets", TabletIndexes)
            .Default();
        RegisterParameter("data_size", DataSize)
            .Default(0);
        RegisterParameter("tablet_count", TabletCount)
            .Default(1);
    }
};

DECLARE_REFCOUNTED_STRUCT(TTestReshardDescriptor)
DEFINE_REFCOUNTED_TYPE(TTestReshardDescriptor)

void CheckEqual(const TTestReshardDescriptorPtr& expected, const TReshardDescriptor& actual, const TTestBundle& bundle, int tableIndex)
{
    EXPECT_EQ(expected->TableIndex, tableIndex);
    EXPECT_EQ(expected->DataSize, actual.DataSize);
    EXPECT_EQ(std::ssize(expected->TabletIndexes), std::ssize(actual.Tablets));
    EXPECT_EQ(expected->TabletCount, actual.TabletCount);

    const auto& tableId = bundle.TableIds[tableIndex];
    const auto& table = GetOrCrash(bundle.Bundle->Tables, tableId);
    for (int tabletIndex = 0; tabletIndex < std::ssize(actual.Tablets); ++tabletIndex) {
        EXPECT_EQ(actual.Tablets[tabletIndex], table->Tablets[tabletIndex]->Id);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TTestTabletBalancingHelpers
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        /*cells*/ TStringBuf,
        /*bundleConfig*/ TStringBuf,
        /*tablets*/ std::vector<std::tuple<
            /*tableSettings*/ TStringBuf,
            /*tableConfig*/ TStringBuf,
            /*tablets*/ std::vector<TStringBuf>>>,
        /*expectedDescriptors*/ TStringBuf>>
{ };

////////////////////////////////////////////////////////////////////////////////

class TTestReassignInMemoryTablets
    : public TTestTabletBalancingHelpers
{ };

TEST_P(TTestReassignInMemoryTablets, Simple)
{
    const auto& params = GetParam();
    auto bundle = CreateTabletCellBundle(std::get<0>(params), std::get<1>(params), std::get<2>(params));

    auto descriptors = ReassignInMemoryTablets(
        bundle.Bundle,
        /*movableTables*/ std::nullopt,
        /*ignoreTableWiseConfig*/ false);

    auto expected = ConvertTo<std::vector<TTestMoveDescriptorPtr>>(TYsonStringBuf(std::get<3>(params)));
    EXPECT_EQ(std::ssize(expected), std::ssize(descriptors));

    std::sort(descriptors.begin(), descriptors.end(), [] (const TMoveDescriptor& lhs, const TMoveDescriptor& rhs) {
        return lhs.TabletId < rhs.TabletId;
    });

    for (int index = 0; index < std::ssize(expected); ++index) {
        EXPECT_TRUE(IsEqual(expected[index], descriptors[index], bundle))
            << "index: " << index << std::endl;
    }
}

INSTANTIATE_TEST_SUITE_P(
    TTestReassignInMemoryTablets,
    TTestReassignInMemoryTablets,
    ::testing::Values(
        std::make_tuple(
            /*cells*/ "[{memory_size=100}; {memory_size=0}]",
            /*bundleConfig*/ "{}",
            /*tables*/ std::vector<std::tuple<TStringBuf, TStringBuf, std::vector<TStringBuf>>>{
                std::make_tuple(
                    /*tableSettings*/ "{sorted=true; in_memory_mode=uncompressed}",
                    /*tableConfig*/ "{}",
                    /*tablets*/ std::vector<TStringBuf>{
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=50}",
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=50}"})},
            /*moveDescriptors*/ "[{tablet_index=0; cell_index=1};]"),
        std::make_tuple(
            /*cells*/ "[{memory_size=100}; {memory_size=0}]",
            /*bundleConfig*/ "{}",
            /*tables*/ std::vector<std::tuple<TStringBuf, TStringBuf, std::vector<TStringBuf>>>{
                std::make_tuple(
                    /*tableSettings*/ "{sorted=true; in_memory_mode=none}",
                    /*tableConfig*/ "{}",
                    /*tablets*/ std::vector<TStringBuf>{
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=50}",
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=50}"})},
            /*moveDescriptors*/ "[]"),
        std::make_tuple(
            /*cells*/ "[{memory_size=150}; {memory_size=0}; {memory_size=0}]",
            /*bundleConfig*/ "{}",
            /*tables*/ std::vector<std::tuple<TStringBuf, TStringBuf, std::vector<TStringBuf>>>{
                std::make_tuple(
                    /*tableSettings*/ "{sorted=true; in_memory_mode=uncompressed}",
                    /*tableConfig*/ "{}",
                    /*tablets*/ std::vector<TStringBuf>{
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=50}",
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=50}",
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=50}"})},
            /*moveDescriptors*/ "[{tablet_index=0; cell_index=1}; {tablet_index=1; cell_index=2};]")));

////////////////////////////////////////////////////////////////////////////////

class TTestReassignOrdinaryTablets
    : public TTestTabletBalancingHelpers
{ };

TEST_P(TTestReassignOrdinaryTablets, Simple)
{
    const auto& params = GetParam();
    auto bundle = CreateTabletCellBundle(std::get<0>(params), std::get<1>(params), std::get<2>(params));

    auto descriptors = ReassignOrdinaryTablets(
        bundle.Bundle,
        /*movableTables*/ std::nullopt);

    auto expected = ConvertTo<std::vector<TTestMoveDescriptorPtr>>(TYsonStringBuf(std::get<3>(params)));
    EXPECT_EQ(std::ssize(expected), std::ssize(descriptors));

    std::sort(descriptors.begin(), descriptors.end(), [] (const TMoveDescriptor& lhs, const TMoveDescriptor& rhs) {
        return lhs.TabletId < rhs.TabletId;
    });

    for (int index = 0; index < std::ssize(expected); ++index) {
        EXPECT_TRUE(IsEqual(expected[index], descriptors[index], bundle))
            << "index: " << index << std::endl;
    }
}

INSTANTIATE_TEST_SUITE_P(
    TTestReassignOrdinaryTablets,
    TTestReassignOrdinaryTablets,
    ::testing::Values(
        std::make_tuple(
            /*cells*/ "[{memory_size=100}; {memory_size=0}]",
            /*bundleConfig*/ "{}",
            /*tables*/ std::vector<std::tuple<TStringBuf, TStringBuf, std::vector<TStringBuf>>>{
                std::make_tuple(
                    /*tableSettings*/ "{sorted=true; in_memory_mode=uncompressed}",
                    /*tableConfig*/ "{}",
                    /*tablets*/ std::vector<TStringBuf>{
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=50}",
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=50}"})},
            /*moveDescriptors*/ "[]"),
        std::make_tuple(
            /*cells*/ "[{memory_size=100}; {memory_size=0}]",
            /*bundleConfig*/ "{}",
            /*tables*/ std::vector<std::tuple<TStringBuf, TStringBuf, std::vector<TStringBuf>>>{
                std::make_tuple(
                    /*tableSettings*/ "{sorted=true; in_memory_mode=none}",
                    /*tableConfig*/ "{}",
                    /*tablets*/ std::vector<TStringBuf>{
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=50}",
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=50}"})},
            /*moveDescriptors*/ "[{tablet_index=1; cell_index=1};]"), 
        std::make_tuple(
            /*cells*/ "[{memory_size=150}; {memory_size=0}; {memory_size=0}]",
            /*bundleConfig*/ "{}",
            /*tables*/ std::vector<std::tuple<TStringBuf, TStringBuf, std::vector<TStringBuf>>>{
                std::make_tuple(
                    /*tableSettings*/ "{sorted=true; in_memory_mode=none}",
                    /*tableConfig*/ "{}",
                    /*tablets*/ std::vector<TStringBuf>{
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=50}",
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=50}",
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=50}"})},
            /*moveDescriptors*/ "[{tablet_index=1; cell_index=1}; {tablet_index=2; cell_index=2};]")));
 
////////////////////////////////////////////////////////////////////////////////

class TTestMergeSplitTabletsOfTable
    : public TTestTabletBalancingHelpers
{ };

TEST_P(TTestMergeSplitTabletsOfTable, Simple)
{
    const auto& params = GetParam();
    auto bundle = CreateTabletCellBundle(std::get<0>(params), std::get<1>(params), std::get<2>(params));

    TTabletBalancerContext context;
    auto expected = ConvertTo<std::vector<TTestReshardDescriptorPtr>>(TYsonStringBuf(std::get<3>(params)));
    auto expectedDescriptorsIt = expected.begin();

    for (int tableIndex = 0; tableIndex < std::ssize(bundle.TableIds); ++tableIndex) {
        const auto& table = bundle.Bundle->Tables[bundle.TableIds[tableIndex]];

        auto descriptors = MergeSplitTabletsOfTable(
            MakeRange(table->Tablets),
            &context);

        for (const auto& descriptor : descriptors) {
            EXPECT_NE(expectedDescriptorsIt, expected.end());
            CheckEqual(*expectedDescriptorsIt, descriptor, bundle, tableIndex);
            ++expectedDescriptorsIt;
        }
    }
    EXPECT_EQ(expectedDescriptorsIt, expected.end());
}

INSTANTIATE_TEST_SUITE_P(
    TTestMergeSplitTabletsOfTable,
    TTestMergeSplitTabletsOfTable,
    ::testing::Values(
        std::make_tuple(
            /*cells*/ "[{memory_size=100}]",
            /*bundleConfig*/ "{}",
            /*tables*/ std::vector<std::tuple<TStringBuf, TStringBuf, std::vector<TStringBuf>>>{
                std::make_tuple(
                    /*tableSettings*/ "{sorted=true}",
                    /*tableConfig*/ "{desired_tablet_count=100}",
                    /*tablets*/ std::vector<TStringBuf>{
                        /*tablet*/ "{uncompressed_data_size=100; memory_size=100}"})},
            /*reshardDescriptors*/ "[{tablets=[0;]; tablet_count=100; data_size=100};]"),
        std::make_tuple(
            /*cells*/ "[{memory_size=300}]",
            /*bundleConfig*/ "{min_tablet_size=200}",
            /*tables*/ std::vector<std::tuple<TStringBuf, TStringBuf, std::vector<TStringBuf>>>{
                std::make_tuple(
                    /*tableSettings*/ "{sorted=true}",
                    /*tableConfig*/ "{}",
                    /*tablets*/ std::vector<TStringBuf>{
                        /*tablet*/ "{uncompressed_data_size=100; memory_size=100}",
                        /*tablet*/ "{uncompressed_data_size=100; memory_size=100}",
                        /*tablet*/ "{uncompressed_data_size=100; memory_size=100}"})},
            /*reshardDescriptors*/ "[{tablets=[0;1;]; tablet_count=1; data_size=200};]")));

////////////////////////////////////////////////////////////////////////////////

class TTestTabletBalancingHelpersWithoutDescriptors
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        /*cells*/ TStringBuf,
        /*bundleConfig*/ TStringBuf,
        /*tablets*/ std::vector<std::tuple<
            /*tableSettings*/ TStringBuf,
            /*tableConfig*/ TStringBuf,
            /*tablets*/ std::vector<TStringBuf>>>>>
{ };

////////////////////////////////////////////////////////////////////////////////

TTabletPtr FindTabletInBundle(const TTabletCellBundlePtr& bundle, TTabletId tabletId)
{
    for (const auto& [tableId, table] : bundle->Tables) {
        for (const auto& tablet : table->Tablets) {
            if (tablet->Id == tabletId) {
                return tablet;
            }
        }
    }
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

class TTestReassignInMemoryTabletsUniform
    : public TTestTabletBalancingHelpersWithoutDescriptors
{ };

TEST_P(TTestReassignInMemoryTabletsUniform, Simple)
{
    const auto& params = GetParam();
    auto bundle = CreateTabletCellBundle(std::get<0>(params), std::get<1>(params), std::get<2>(params));

    auto descriptors = ReassignInMemoryTablets(
        bundle.Bundle,
        /*movableTables*/ std::nullopt,
        /*ignoreTableWiseConfig*/ false);

    i64 totalSize = 0;
    THashMap<TTabletCellId, i64> cellSizes;
    for (const auto& cell : bundle.Cells) {
        EmplaceOrCrash(cellSizes, cell->Id, cell->Statistics.MemorySize);
        totalSize += cell->Statistics.MemorySize;
    }

    for (const auto& descriptor : descriptors) {
        auto tablet = FindTabletInBundle(bundle.Bundle, descriptor.TabletId);
        EXPECT_TRUE(tablet != nullptr);
        EXPECT_TRUE(tablet->Cell != nullptr);
        EXPECT_TRUE(tablet->Cell->Id != descriptor.TabletCellId);
        EXPECT_TRUE(tablet->Table->InMemoryMode != EInMemoryMode::None);
        auto tabletSize = GetTabletBalancingSize(tablet);
        cellSizes[tablet->Cell->Id] -= tabletSize;
        cellSizes[descriptor.TabletCellId] += tabletSize;
    }

    EXPECT_FALSE(cellSizes.empty());
    EXPECT_TRUE(totalSize % cellSizes.size() == 0);
    i64 expectedSize = totalSize / cellSizes.size();
    for (const auto& [cellId, memorySize] : cellSizes) {
        EXPECT_EQ(memorySize, expectedSize)
            << "cell_id: " << ToString(cellId) << std::endl;
    }
}

INSTANTIATE_TEST_SUITE_P(
    TTestReassignInMemoryTabletsUniform,
    TTestReassignInMemoryTabletsUniform,
    ::testing::Values(
        std::make_tuple(
            /*cells*/ "[{memory_size=100}; {memory_size=0}]",
            /*bundleConfig*/ "{}",
            /*tables*/ std::vector<std::tuple<TStringBuf, TStringBuf, std::vector<TStringBuf>>>{
                std::make_tuple(
                    /*tableSettings*/ "{sorted=true; in_memory_mode=uncompressed}",
                    /*tableConfig*/ "{}",
                    /*tablets*/ std::vector<TStringBuf>{
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=50}",
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=50}"})}),
        std::make_tuple(
            /*cells*/ "[{memory_size=150}; {memory_size=0}; {memory_size=0}]",
            /*bundleConfig*/ "{}",
            /*tables*/ std::vector<std::tuple<TStringBuf, TStringBuf, std::vector<TStringBuf>>>{
                std::make_tuple(
                    /*tableSettings*/ "{sorted=true; in_memory_mode=uncompressed}",
                    /*tableConfig*/ "{}",
                    /*tablets*/ std::vector<TStringBuf>{
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=50}",
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=50}",
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=50}"})})));

////////////////////////////////////////////////////////////////////////////////

class TTestReassignOrdinaryTabletsUniform
    : public TTestTabletBalancingHelpersWithoutDescriptors
{ };

TEST_P(TTestReassignOrdinaryTabletsUniform, Simple)
{
    const auto& params = GetParam();
    auto bundle = CreateTabletCellBundle(std::get<0>(params), std::get<1>(params), std::get<2>(params));

    auto descriptors = ReassignOrdinaryTablets(
        bundle.Bundle,
        /*movableTables*/ std::nullopt);

    i64 totalSize = 0;
    THashMap<TTabletCellId, i64> cellSizes;
    for (const auto& cell : bundle.Cells) {
        EmplaceOrCrash(cellSizes, cell->Id, cell->Statistics.MemorySize);
        totalSize += cell->Statistics.MemorySize;
    }

    for (const auto& descriptor : descriptors) {
        auto tablet = FindTabletInBundle(bundle.Bundle, descriptor.TabletId);
        EXPECT_TRUE(tablet != nullptr);
        EXPECT_TRUE(tablet->Cell != nullptr);
        EXPECT_TRUE(tablet->Cell->Id != descriptor.TabletCellId);
        EXPECT_TRUE(tablet->Table->InMemoryMode == EInMemoryMode::None);
        auto tabletSize = GetTabletBalancingSize(tablet);
        cellSizes[tablet->Cell->Id] -= tabletSize;
        cellSizes[descriptor.TabletCellId] += tabletSize;
    }

    EXPECT_FALSE(cellSizes.empty());
    EXPECT_TRUE(totalSize % cellSizes.size() == 0);
    i64 expectedSize = totalSize / cellSizes.size();
    for (const auto& [cellId, memorySize] : cellSizes) {
        EXPECT_EQ(memorySize, expectedSize)
            << "cell_id: " << ToString(cellId) << std::endl;
    }
}

INSTANTIATE_TEST_SUITE_P(
    TTestReassignOrdinaryTabletsUniform,
    TTestReassignOrdinaryTabletsUniform,
    ::testing::Values(
        std::make_tuple(
            /*cells*/ "[{memory_size=100}; {memory_size=0}]",
            /*bundleConfig*/ "{}",
            /*tables*/ std::vector<std::tuple<TStringBuf, TStringBuf, std::vector<TStringBuf>>>{
                std::make_tuple(
                    /*tableSettings*/ "{sorted=true; in_memory_mode=none}",
                    /*tableConfig*/ "{}",
                    /*tablets*/ std::vector<TStringBuf>{
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=0}",
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=0}"})}), 
        std::make_tuple(
            /*cells*/ "[{memory_size=150}; {memory_size=0}; {memory_size=0}]",
            /*bundleConfig*/ "{}",
            /*tables*/ std::vector<std::tuple<TStringBuf, TStringBuf, std::vector<TStringBuf>>>{
                std::make_tuple(
                    /*tableSettings*/ "{sorted=true; in_memory_mode=none}",
                    /*tableConfig*/ "{}",
                    /*tablets*/ std::vector<TStringBuf>{
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=0}",
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=0}",
                        /*tablet*/ "{uncompressed_data_size=50; cell_index=0; memory_size=0}"})})));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletBalancer

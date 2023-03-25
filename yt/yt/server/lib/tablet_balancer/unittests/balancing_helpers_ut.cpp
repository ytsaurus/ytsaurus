#include <yt/yt/server/lib/tablet_balancer/balancing_helpers.h>
#include <yt/yt/server/lib/tablet_balancer/config.h>
#include <yt/yt/server/lib/tablet_balancer/parameterized_balancing_helpers.h>
#include <yt/yt/server/lib/tablet_balancer/table.h>
#include <yt/yt/server/lib/tablet_balancer/tablet.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell_bundle.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTabletBalancer {
namespace {

using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("BalancingHelpersUnittest");

////////////////////////////////////////////////////////////////////////////////

TObjectId MakeSequentialId(EObjectType type)
{
    static int counter = 0;
    return MakeId(type, /*cellTag*/ MinValidCellTag, /*counter*/ 0, /*hash*/ counter++);
}

struct TTestTablet
    : public TYsonStruct
{
    i64 UncompressedDataSize;
    i64 MemorySize;
    int CellIndex;

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
        tablet->Statistics.OriginalNode = BuildYsonNodeFluently()
            .DoMap([&] (TFluentMap fluent) {
                fluent
                    .Item("memory_size").Value(MemorySize)
                    .Item("uncompressed_data_size").Value(UncompressedDataSize);
        });

        tablet->Cell = cells[CellIndex].Get();
        EmplaceOrCrash(tablet->Cell->Tablets, tabletId, tablet);

        return tablet;
    }

    REGISTER_YSON_STRUCT(TTestTablet);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("uncompressed_data_size", &TThis::UncompressedDataSize)
            .Default(100);
        registrar.Parameter("memory_size", &TThis::MemorySize)
            .Default(100);
        registrar.Parameter("cell_index", &TThis::CellIndex)
            .Default(0);
    }
};

DECLARE_REFCOUNTED_STRUCT(TTestTablet)
DEFINE_REFCOUNTED_TYPE(TTestTablet)

////////////////////////////////////////////////////////////////////////////////

struct TTestTable
    : public TYsonStruct
{
    bool Sorted;
    i64 CompressedDataSize;
    i64 UncompressedDataSize;
    EInMemoryMode InMemoryMode;

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
        table->InMemoryMode = InMemoryMode;
        return table;
    }

    REGISTER_YSON_STRUCT(TTestTable);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("sorted", &TThis::Sorted)
            .Default(true);
        registrar.Parameter("compressed_data_size", &TThis::CompressedDataSize)
            .Default(100);
        registrar.Parameter("uncompressed_data_size", &TThis::UncompressedDataSize)
            .Default(100);
        registrar.Parameter("in_memory_mode", &TThis::InMemoryMode)
            .Default(EInMemoryMode::None);
    }
};

DECLARE_REFCOUNTED_STRUCT(TTestTable)
DEFINE_REFCOUNTED_TYPE(TTestTable)

////////////////////////////////////////////////////////////////////////////////

struct TTestNode
    : public TYsonStruct
{
    TString NodeAddress;
    i64 Used;
    std::optional<i64> Limit;

    REGISTER_YSON_STRUCT(TTestNode);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("node_address", &TThis::NodeAddress)
            .Default();
        registrar.Parameter("used", &TThis::Used)
            .Default(0);
        registrar.Parameter("limit", &TThis::Limit)
            .Default();
    }
};

DECLARE_REFCOUNTED_STRUCT(TTestNode)
DEFINE_REFCOUNTED_TYPE(TTestNode)

////////////////////////////////////////////////////////////////////////////////

struct TTestTabletCell
    : public TYsonStruct
{
    i64 MemorySize;
    std::optional<TString> NodeAddress;

    REGISTER_YSON_STRUCT(TTestTabletCell);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("memory_size", &TThis::MemorySize)
            .Default(100);
        registrar.Parameter("node_address", &TThis::NodeAddress)
            .Default();
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
    THashMap<TTabletId, TTabletPtr> Tablets;
};

using TTestTabletParams = TStringBuf;

struct TTestTableParams
{
    TStringBuf TableSettings;
    TStringBuf TableConfig;
    std::vector<TTestTabletParams> Tablets;
};

struct TTestBundleParams
{
    TStringBuf Nodes = "[]";
    TStringBuf Cells;
    TStringBuf BundleConfig;
    std::vector<TTestTableParams> Tables;
};

////////////////////////////////////////////////////////////////////////////////

TTestBundle CreateTabletCellBundle(
    const TTestBundleParams& bundleParams)
{
    TTestBundle testBundle;
    testBundle.Bundle = New<TTabletCellBundle>("bundle");
    testBundle.Bundle->Config = ConvertTo<TBundleTabletBalancerConfigPtr>(TYsonStringBuf(bundleParams.BundleConfig));
    testBundle.Bundle->Config->EnableVerboseLogging = true;

    auto nodes = ConvertTo<std::vector<TTestNodePtr>>(TYsonStringBuf(bundleParams.Nodes));
    for (const auto& node : nodes) {
        auto statistics = TTabletCellBundle::TNodeMemoryStatistics{.Used = node->Used};
        if (node->Limit.has_value()) {
            statistics.Limit = *node->Limit;
        }

        EmplaceOrCrash(
            testBundle.Bundle->NodeMemoryStatistics,
            node->NodeAddress,
            std::move(statistics));
    }

    std::vector<TTabletCellId> cellIds;
    auto cells = ConvertTo<std::vector<TTestTabletCellPtr>>(TYsonStringBuf(bundleParams.Cells));
    for (int index = 0; index < std::ssize(cells); ++index) {
        cellIds.emplace_back(MakeSequentialId(EObjectType::TabletCell));
    }

    for (int index = 0; index < std::ssize(cells); ++index) {
        if (cells[index]->NodeAddress.has_value()) {
            EXPECT_TRUE(testBundle.Bundle->NodeMemoryStatistics.contains(*cells[index]->NodeAddress));
        }

        auto cell = New<TTabletCell>(
            cellIds[index],
            TTabletCellStatistics{.MemorySize = cells[index]->MemorySize},
            TTabletCellStatus{.Health = NTabletClient::ETabletCellHealth::Good, .Decommissioned = false},
            cells[index]->NodeAddress,
            ETabletCellLifeStage::Running);
        testBundle.Bundle->TabletCells.emplace(cellIds[index], cell);
        testBundle.Cells.push_back(cell);
    }

    for (const auto& [tableSettings, tableConfig, tablets] : bundleParams.Tables) {
        auto table = ConvertTo<TTestTablePtr>(TYsonStringBuf(tableSettings))->CreateTable(testBundle.Bundle);

        table->TableConfig = ConvertTo<TTableTabletBalancerConfigPtr>(TYsonStringBuf(tableConfig));
        table->TableConfig->EnableVerboseLogging = true;

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
            table->Tablets.push_back(tablet);
            EmplaceOrCrash(testBundle.Tablets, tabletIds[index], std::move(tablet));
        }

        EmplaceOrCrash(testBundle.Bundle->Tables, table->Id, table);
        testBundle.TableIds.push_back(table->Id);
    }

    for (const auto& [id, cell] : testBundle.Bundle->TabletCells) {
        i64 tabletsSize = 0;
        for (const auto& [tabletId, tablet] : cell->Tablets) {
            tabletsSize += GetTabletBalancingSize(tablet);
        }

        EXPECT_LE(tabletsSize, cell->Statistics.MemorySize);
    }

    return testBundle;
}

////////////////////////////////////////////////////////////////////////////////

struct TTestMoveDescriptor
    : public TYsonStruct
{
    int TableIndex;
    int TabletIndex;
    int CellIndex;

    REGISTER_YSON_STRUCT(TTestMoveDescriptor);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("table_index", &TThis::TableIndex)
            .Default(0);
        registrar.Parameter("tablet_index", &TThis::TabletIndex)
            .Default(0);
        registrar.Parameter("cell_index", &TThis::CellIndex)
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
    : public TYsonStruct
{
    int TableIndex;
    std::vector<int> TabletIndexes;
    i64 DataSize;
    int TabletCount;

    REGISTER_YSON_STRUCT(TTestReshardDescriptor);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("table_index", &TThis::TableIndex)
            .Default(0);
        registrar.Parameter("tablets", &TThis::TabletIndexes)
            .Default();
        registrar.Parameter("data_size", &TThis::DataSize)
            .Default(0);
        registrar.Parameter("tablet_count", &TThis::TabletCount)
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
        /*bundle*/ TTestBundleParams,
        /*expectedDescriptors*/ TStringBuf>>
{ };

////////////////////////////////////////////////////////////////////////////////

class TTestReassignInMemoryTablets
    : public TTestTabletBalancingHelpers
{ };

TEST_P(TTestReassignInMemoryTablets, SimpleWithSameTablets)
{
    const auto& params = GetParam();
    auto bundle = CreateTabletCellBundle(std::get<0>(params));

    auto descriptors = ReassignInMemoryTablets(
        bundle.Bundle,
        /*movableTables*/ std::nullopt,
        /*ignoreTableWiseConfig*/ false,
        Logger);

    auto expected = ConvertTo<std::vector<TTestMoveDescriptorPtr>>(TYsonStringBuf(std::get<1>(params)));
    EXPECT_EQ(std::ssize(expected), std::ssize(descriptors));

    std::sort(descriptors.begin(), descriptors.end(), [] (const TMoveDescriptor& lhs, const TMoveDescriptor& rhs) {
        return lhs.TabletId < rhs.TabletId;
    });

    for (int index = 0; index < std::ssize(expected); ++index) {
        EXPECT_TRUE(IsEqual(expected[index], descriptors[index], bundle))
            << "index: " << index << Endl;
    }
}

INSTANTIATE_TEST_SUITE_P(
    TTestReassignInMemoryTablets,
    TTestReassignInMemoryTablets,
    ::testing::Values(
        std::make_tuple(
            TTestBundleParams{
                .Cells = "[{memory_size=100}; {memory_size=0}]",
                .BundleConfig = "{}",
                .Tables = std::vector<TTestTableParams>{
                    TTestTableParams{
                        .TableSettings = "{sorted=%true; in_memory_mode=uncompressed}",
                        .TableConfig = "{}",
                        .Tablets = std::vector<TStringBuf>{
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"},
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"}}}}},
            /*moveDescriptors*/ "[{tablet_index=0; cell_index=1};]"),
        std::make_tuple(
            TTestBundleParams{
                .Cells = "[{memory_size=100}; {memory_size=0}]",
                .BundleConfig = "{}",
                .Tables = std::vector<TTestTableParams>{
                    TTestTableParams{
                        .TableSettings = "{sorted=%true; in_memory_mode=none}",
                        .TableConfig = "{}",
                        .Tablets = std::vector<TStringBuf>{
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"},
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"}}}}},
            /*moveDescriptors*/ "[]"),
        std::make_tuple(
            TTestBundleParams{
                .Cells = "[{memory_size=150}; {memory_size=0}; {memory_size=0}]",
                .BundleConfig = "{}",
                .Tables = std::vector<TTestTableParams>{
                    TTestTableParams{
                        .TableSettings = "{sorted=%true; in_memory_mode=uncompressed}",
                        .TableConfig = "{}",
                        .Tablets = std::vector<TStringBuf>{
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"},
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"},
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"}}}}},
            /*moveDescriptors*/ "[{tablet_index=0; cell_index=1}; {tablet_index=1; cell_index=2};]")));

////////////////////////////////////////////////////////////////////////////////

class TTestReassignOrdinaryTablets
    : public TTestTabletBalancingHelpers
{ };

TEST_P(TTestReassignOrdinaryTablets, Simple)
{
    const auto& params = GetParam();
    auto bundle = CreateTabletCellBundle(std::get<0>(params));

    auto descriptors = ReassignOrdinaryTablets(
        bundle.Bundle,
        /*movableTables*/ std::nullopt,
        Logger);

    auto expected = ConvertTo<std::vector<TTestMoveDescriptorPtr>>(TYsonStringBuf(std::get<1>(params)));
    EXPECT_EQ(std::ssize(expected), std::ssize(descriptors));

    EXPECT_EQ(std::ssize(bundle.Bundle->Tables), 1);
    EXPECT_EQ(std::ssize(bundle.TableIds), 1);

    auto table = bundle.Bundle->Tables[bundle.TableIds[0]];

    const auto& anyTablet = table->Tablets[0];
    THashMap<TTabletCellId, THashSet<TTabletId>> cellToTablets;
    for (const auto& cell : bundle.Cells) {
        EmplaceOrCrash(cellToTablets, cell->Id, THashSet<TTabletId>{});
    }

    for (const auto& tablet : table->Tablets) {
        EXPECT_EQ(tablet->Statistics.MemorySize, anyTablet->Statistics.MemorySize);
        EXPECT_EQ(tablet->Statistics.UncompressedDataSize, anyTablet->Statistics.UncompressedDataSize);
        EXPECT_EQ(tablet->Cell, anyTablet->Cell);
        InsertOrCrash(cellToTablets[tablet->Cell->Id], tablet->Id);
    }

    auto expectedCellToTablets = cellToTablets;
    for (const auto& descriptor : descriptors) {
        const auto& tablet = GetOrCrash(bundle.Tablets, descriptor.TabletId);

        InsertOrCrash(cellToTablets[descriptor.TabletCellId], descriptor.TabletId);
        EraseOrCrash(cellToTablets[tablet->Cell->Id], descriptor.TabletId);
    }

    for (const auto& descriptor : expected) {
        const auto& cell = bundle.Cells[descriptor->CellIndex];
        const auto& tablet = table->Tablets[descriptor->TabletIndex];

        InsertOrCrash(expectedCellToTablets[cell->Id], tablet->Id);
        EraseOrCrash(expectedCellToTablets[tablet->Cell->Id], tablet->Id);
    }

    for (const auto& [cellId, tablets] : cellToTablets) {
        const auto& expected = GetOrCrash(expectedCellToTablets, cellId);
        EXPECT_EQ(std::ssize(tablets), std::ssize(expected))
            << "cellId: " << ToString(cellId) << Endl;
    }
}

INSTANTIATE_TEST_SUITE_P(
    TTestReassignOrdinaryTablets,
    TTestReassignOrdinaryTablets,
    ::testing::Values(
        std::make_tuple(
            TTestBundleParams{
                .Cells = "[{memory_size=100}; {memory_size=0}]",
                .BundleConfig = "{}",
                .Tables = std::vector<TTestTableParams>{
                    TTestTableParams{
                        .TableSettings = "{sorted=%true; in_memory_mode=uncompressed}",
                        .TableConfig = "{}",
                        .Tablets = std::vector<TStringBuf>{
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"},
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"}}}}},
            /*moveDescriptors*/ "[]"),
        std::make_tuple(
            TTestBundleParams{
                .Cells = "[{memory_size=100}; {memory_size=0}]",
                .BundleConfig = "{}",
                .Tables = std::vector<TTestTableParams>{
                    TTestTableParams{
                        .TableSettings = "{sorted=%true; in_memory_mode=none}",
                        .TableConfig = "{}",
                        .Tablets = std::vector<TStringBuf>{
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"},
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"}}}}},
            /*moveDescriptors*/ "[{tablet_index=1; cell_index=1};]"),
        std::make_tuple(
            TTestBundleParams{
                .Cells = "[{memory_size=150}; {memory_size=0}; {memory_size=0}]",
                .BundleConfig = "{}",
                .Tables = std::vector<TTestTableParams>{
                    TTestTableParams{
                        .TableSettings = "{sorted=%true; in_memory_mode=none}",
                        .TableConfig = "{}",
                        .Tablets = std::vector<TStringBuf>{
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"},
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"},
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"}}}}},
            /*moveDescriptors*/ "[{tablet_index=1; cell_index=1}; {tablet_index=2; cell_index=2};]")));

////////////////////////////////////////////////////////////////////////////////

class TTestMergeSplitTabletsOfTable
    : public TTestTabletBalancingHelpers
{ };

TEST_P(TTestMergeSplitTabletsOfTable, Simple)
{
    const auto& params = GetParam();
    auto bundle = CreateTabletCellBundle(std::get<0>(params));

    auto expected = ConvertTo<std::vector<TTestReshardDescriptorPtr>>(TYsonStringBuf(std::get<1>(params)));
    auto expectedDescriptorsIt = expected.begin();

    for (int tableIndex = 0; tableIndex < std::ssize(bundle.TableIds); ++tableIndex) {
        const auto& table = bundle.Bundle->Tables[bundle.TableIds[tableIndex]];

        auto descriptors = MergeSplitTabletsOfTable(
            table->Tablets,
            Logger);

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
            TTestBundleParams{
                .Cells = "[{memory_size=100}]",
                .BundleConfig = "{}",
                .Tables = std::vector<TTestTableParams>{
                    TTestTableParams{
                        .TableSettings = "{sorted=%true}",
                        .TableConfig = "{desired_tablet_count=100}",
                        .Tablets = std::vector<TStringBuf>{
                            TTestTabletParams{"{uncompressed_data_size=100; memory_size=100}"}}}}},
            /*reshardDescriptors*/ "[{tablets=[0;]; tablet_count=100; data_size=100};]"),
        std::make_tuple(
            TTestBundleParams{
                .Cells = "[{memory_size=300}]",
                .BundleConfig = "{min_tablet_size=200}",
                .Tables = std::vector<TTestTableParams>{
                    TTestTableParams{
                        .TableSettings = "{sorted=%true}",
                        .TableConfig = "{}",
                        .Tablets = std::vector<TStringBuf>{
                            TTestTabletParams{"{uncompressed_data_size=100; memory_size=100}"},
                            TTestTabletParams{"{uncompressed_data_size=100; memory_size=100}"},
                            TTestTabletParams{"{uncompressed_data_size=100; memory_size=100}"}}}}},
            /*reshardDescriptors*/ "[{tablets=[0;1;]; tablet_count=1; data_size=200};]")));

////////////////////////////////////////////////////////////////////////////////

class TTestTabletBalancingHelpersWithoutDescriptors
    : public ::testing::Test
    , public ::testing::WithParamInterface<TTestBundleParams>
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
    auto bundle = CreateTabletCellBundle(params);

    auto descriptors = ReassignInMemoryTablets(
        bundle.Bundle,
        /*movableTables*/ std::nullopt,
        /*ignoreTableWiseConfig*/ false,
        Logger);

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
        auto tabletSize = GetTabletBalancingSize(tablet.Get());
        cellSizes[tablet->Cell->Id] -= tabletSize;
        cellSizes[descriptor.TabletCellId] += tabletSize;
    }

    EXPECT_FALSE(cellSizes.empty());
    EXPECT_TRUE(totalSize % cellSizes.size() == 0);
    i64 expectedSize = totalSize / cellSizes.size();
    for (const auto& [cellId, memorySize] : cellSizes) {
        EXPECT_EQ(memorySize, expectedSize)
            << "cell_id: " << ToString(cellId) << Endl;
    }
}

INSTANTIATE_TEST_SUITE_P(
    TTestReassignInMemoryTabletsUniform,
    TTestReassignInMemoryTabletsUniform,
    ::testing::Values(
        TTestBundleParams{
            .Cells = "[{memory_size=100}; {memory_size=0}]",
            .BundleConfig = "{}",
            .Tables = std::vector<TTestTableParams>{
                TTestTableParams{
                    .TableSettings = "{sorted=%true; in_memory_mode=uncompressed}",
                    .TableConfig = "{}",
                    .Tablets = std::vector<TStringBuf>{
                        TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"},
                        TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"}}}}},
        TTestBundleParams{
            .Cells = "[{memory_size=150}; {memory_size=0}; {memory_size=0}]",
            .BundleConfig = "{}",
            .Tables = std::vector<TTestTableParams>{
                TTestTableParams{
                    .TableSettings = "{sorted=%true; in_memory_mode=uncompressed}",
                    .TableConfig = "{}",
                    .Tablets = std::vector<TStringBuf>{
                        TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"},
                        TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"},
                        TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"}}}}}));

////////////////////////////////////////////////////////////////////////////////

class TTestReassignOrdinaryTabletsUniform
    : public TTestTabletBalancingHelpersWithoutDescriptors
{ };

TEST_P(TTestReassignOrdinaryTabletsUniform, Simple)
{
    const auto& params = GetParam();
    auto bundle = CreateTabletCellBundle(params);

    auto descriptors = ReassignOrdinaryTablets(
        bundle.Bundle,
        /*movableTables*/ std::nullopt,
        Logger);

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
        auto tabletSize = GetTabletBalancingSize(tablet.Get());
        cellSizes[tablet->Cell->Id] -= tabletSize;
        cellSizes[descriptor.TabletCellId] += tabletSize;
    }

    EXPECT_FALSE(cellSizes.empty());
    EXPECT_TRUE(totalSize % cellSizes.size() == 0);
    i64 expectedSize = totalSize / cellSizes.size();
    for (const auto& [cellId, memorySize] : cellSizes) {
        EXPECT_EQ(memorySize, expectedSize)
            << "cellId: " << ToString(cellId) << Endl;
    }
}

INSTANTIATE_TEST_SUITE_P(
    TTestReassignOrdinaryTabletsUniform,
    TTestReassignOrdinaryTabletsUniform,
    ::testing::Values(
        TTestBundleParams{
            .Cells = "[{memory_size=100}; {memory_size=0}]",
            .BundleConfig = "{}",
            .Tables = std::vector<TTestTableParams>{
                TTestTableParams{
                    .TableSettings = "{sorted=%true; in_memory_mode=none}",
                    .TableConfig = "{}",
                    .Tablets = std::vector<TStringBuf>{
                        TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=0}"},
                        TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=0}"}}}}},
        TTestBundleParams{
            .Cells = "[{memory_size=150}; {memory_size=0}; {memory_size=0}]",
            .BundleConfig = "{}",
            .Tables = std::vector<TTestTableParams>{
                TTestTableParams{
                    .TableSettings = "{sorted=%true; in_memory_mode=none}",
                    .TableConfig = "{}",
                    .Tablets = std::vector<TStringBuf>{
                        TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=0}"},
                        TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=0}"},
                        TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=0}"}}}}}));

////////////////////////////////////////////////////////////////////////////////

class TTestReassignTabletsParameterized
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        /*bundle*/ TTestBundleParams,
        /*expectedDescriptors*/ TStringBuf,
        /*moveActionLimit*/ int,
        /*distribution*/ std::vector<int>,
        /*cellSizes*/ std::vector<i64>>>
{ };

TEST_P(TTestReassignTabletsParameterized, SimpleViaMemorySize)
{
    const auto& params = GetParam();
    auto bundle = CreateTabletCellBundle(std::get<0>(params));

    const auto& table = GetOrCrash(bundle.Bundle->Tables, bundle.TableIds[0]);
    auto group = table->TableConfig->Group.value_or(DefaultGroupName);

    auto descriptors = ReassignTabletsParameterized(
        bundle.Bundle,
        /*performanceCountersKeys*/ {},
        TParameterizedReassignSolverConfig{
            .MaxMoveActionCount = std::get<2>(params)
        }.MergeWith(GetOrCrash(bundle.Bundle->Config->Groups, group)->Parameterized),
        group,
        Logger);

    auto expected = ConvertTo<std::vector<TTestMoveDescriptorPtr>>(TYsonStringBuf(std::get<1>(params)));
    EXPECT_EQ(std::ssize(expected), std::ssize(descriptors));

    THashMap<TTabletId, TTabletCellId> tabletToCell;
    for (const auto& cell : bundle.Cells) {
        for (const auto& [tabletId, tablet] : cell->Tablets) {
            EmplaceOrCrash(tabletToCell, tabletId, cell->Id);
        }
    }

    for (const auto& descriptor : descriptors) {
        auto tablet = FindTabletInBundle(bundle.Bundle, descriptor.TabletId);
        EXPECT_TRUE(tablet != nullptr);
        EXPECT_TRUE(tablet->Cell != nullptr);
        EXPECT_TRUE(tablet->Cell->Id != descriptor.TabletCellId);

        tabletToCell[descriptor.TabletId] = descriptor.TabletCellId;
    }

    THashMap<TTabletCellId, int> tabletCounts;
    for (auto [tabletId, cellId] : tabletToCell) {
        ++tabletCounts[cellId];
    }

    auto expectedDistribution = std::get<3>(params);
    std::sort(expectedDistribution.begin(), expectedDistribution.end());

    std::vector<int> actualDistribution;
    for (auto [_, tabletCount] : tabletCounts) {
        actualDistribution.emplace_back(tabletCount);
    }

    actualDistribution.resize(std::ssize(expectedDistribution));
    std::sort(actualDistribution.begin(), actualDistribution.end());

    EXPECT_EQ(expectedDistribution, actualDistribution);

    THashMap<TTabletCellId, i64> cellToSize;
    for (auto [tabletId, cellId] : tabletToCell) {
        auto tablet = FindTabletInBundle(bundle.Bundle, tabletId);
        cellToSize[cellId] += tablet->Statistics.MemorySize;
    }

    auto expectedSizes = std::get<4>(params);
    std::sort(expectedSizes.begin(), expectedSizes.end());

    std::vector<i64> cellSizes;
    for (auto [_, size] : cellToSize) {
        cellSizes.emplace_back(size);
    }

    cellSizes.resize(std::ssize(expectedSizes));

    std::sort(cellSizes.begin(), cellSizes.end());
    EXPECT_EQ(cellSizes, expectedSizes);
}

INSTANTIATE_TEST_SUITE_P(
    TTestReassignTabletsParameterized,
    TTestReassignTabletsParameterized,
    ::testing::Values(
        std::make_tuple( // NO ACTIONS
            TTestBundleParams{
                .Nodes = "[{node_address=home; used=100}]",
                .Cells = "[{memory_size=100; node_address=home}; {memory_size=0; node_address=home}]",
                .BundleConfig = "{groups={rex={parameterized={metric=\"int64([/statistics/memory_size]) + int64([/statistics/uncompressed_data_size])\"}}}}",
                .Tables = std::vector<TTestTableParams>{
                    TTestTableParams{
                        .TableSettings = "{sorted=%true; in_memory_mode=uncompressed}",
                        .TableConfig = "{enable_parameterized=%true; group=rex}",
                        .Tablets = std::vector<TStringBuf>{
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"},
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"}}}}},
            /*moveDescriptors*/ "[]",
            /*moveActionLimit*/ 0,
            /*distribution*/ std::vector<int>{2, 0},
            /*cellSizes*/ std::vector<i64>{100, 0}),
        std::make_tuple( // MOVE
            TTestBundleParams{
                .Nodes = "[{node_address=home; used=100; limit=100}]",
                .Cells = "[{memory_size=100; node_address=home}; {memory_size=0; node_address=home}]",
                .BundleConfig = "{groups={default={parameterized={metric=\"double([/statistics/memory_size])\"}}}}",
                .Tables = std::vector<TTestTableParams>{
                    TTestTableParams{
                        .TableSettings = "{sorted=%true; in_memory_mode=uncompressed}",
                        .TableConfig = "{enable_parameterized=%true}",
                        .Tablets = std::vector<TStringBuf>{
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=60}"},
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=40}"}}}}},
            /*moveDescriptors*/ "[{tablet_index=1; cell_index=1};]",
            /*moveActionLimit*/ 1,
            /*distribution*/ std::vector<int>{1, 1},
            /*cellSizes*/ std::vector<i64>{60, 40}),
        std::make_tuple( // MOVE (group)
            TTestBundleParams{
                .Nodes = "[{node_address=home; used=0; limit=200}]",
                .Cells = "[{memory_size=100; node_address=home}; {memory_size=0; node_address=home}]",
                .BundleConfig = "{groups={rex={parameterized={metric=\"0\"}}; "
                    "fex={parameterized={metric=\"int64([/statistics/memory_size]) + int64([/statistics/uncompressed_data_size])\"}}}}",
                .Tables = std::vector<TTestTableParams>{
                    TTestTableParams{
                        .TableSettings = "{sorted=%true; in_memory_mode=uncompressed}",
                        .TableConfig = "{group=fex; enable_parameterized=%true}",
                        .Tablets = std::vector<TStringBuf>{
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=60}"},
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=40}"}}}}},
            /*moveDescriptors*/ "[{tablet_index=1; cell_index=1};]",
            /*moveActionLimit*/ 1,
            /*distribution*/ std::vector<int>{1, 1},
            /*cellSizes*/ std::vector<i64>{60, 40}),
        std::make_tuple( // SWAP (available action count is more than needed)
            TTestBundleParams{
                .Nodes = "[{node_address=home; used=0; limit=200}]",
                .Cells = "[{memory_size=70; node_address=home}; {memory_size=50; node_address=home}]",
                .BundleConfig = "{enable_parameterized_by_default=%true; groups={default={parameterized={metric=\"double([/statistics/memory_size])\"}}}}",
                .Tables = std::vector<TTestTableParams>{
                    TTestTableParams{
                        .TableSettings = "{sorted=%true; in_memory_mode=uncompressed}",
                        .TableConfig = "{}",
                        .Tablets = std::vector<TStringBuf>{
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"},
                            TTestTabletParams{"{uncompressed_data_size=20; cell_index=0; memory_size=20}"},
                            TTestTabletParams{"{uncompressed_data_size=10; cell_index=1; memory_size=10}"},
                            TTestTabletParams{"{uncompressed_data_size=40; cell_index=1; memory_size=40}"}}}}},
            /*moveDescriptors*/ "[{tablet_index=1; cell_index=1}; {tablet_index=2; cell_index=0};]",
            /*moveActionLimit*/ 3,
            /*distribution*/ std::vector<int>{2, 2},
            /*cellSizes*/ std::vector<i64>{60, 60}),
        std::make_tuple( // DISABLE BALANCING
            TTestBundleParams{
                .Nodes = "[{node_address=home; used=100; limit=200}]",
                .Cells = "[{memory_size=100; node_address=home}; {memory_size=0; node_address=home}]",
                .BundleConfig = "{groups={default={parameterized={metric=\"double([/statistics/memory_size])\"}}}}",
                .Tables = std::vector<TTestTableParams>{
                    TTestTableParams{
                        .TableSettings = "{sorted=%true; in_memory_mode=uncompressed}",
                        .TableConfig = "{}",
                        .Tablets = std::vector<TStringBuf>{
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"},
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"}}}}},
            /*moveDescriptors*/ "[]",
            /*moveActionLimit*/ 3,
            /*distribution*/ std::vector<int>{2, 0},
            /*cellSizes*/ std::vector<i64>{100, 0}),
        std::make_tuple( // DISABLE BALANCING HARD
            TTestBundleParams{
                .Nodes = "[{node_address=home; used=100; limit=200}]",
                .Cells = "[{memory_size=100; node_address=home}; {memory_size=0; node_address=home}]",
                .BundleConfig = "{groups={default={parameterized={metric=\"1\"}}}}",
                .Tables = std::vector<TTestTableParams>{
                    TTestTableParams{
                        .TableSettings = "{sorted=%true; in_memory_mode=uncompressed}",
                        .TableConfig = "{enable_parameterized=%false}",
                        .Tablets = std::vector<TStringBuf>{
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"},
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"}}}}},
            /*moveDescriptors*/ "[]",
            /*moveActionLimit*/ 3,
            /*distribution*/ std::vector<int>{2, 0},
            /*cellSizes*/ std::vector<i64>{100, 0})));

////////////////////////////////////////////////////////////////////////////////

class TTestReassignTabletsParameterizedErrors
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        /*bundle*/ TTestBundleParams,
        /*expectedDescriptors*/ TString>>
{ };

TEST_P(TTestReassignTabletsParameterizedErrors, BalancingError)
{
    const auto& params = GetParam();
    auto bundle = CreateTabletCellBundle(std::get<0>(params));

    const auto& table = GetOrCrash(bundle.Bundle->Tables, bundle.TableIds[0]);
    auto group = table->TableConfig->Group.value_or(DefaultGroupName);

    EXPECT_THROW_WITH_SUBSTRING(
        ReassignTabletsParameterized(
            bundle.Bundle,
            /*performanceCountersKeys*/ {},
            TParameterizedReassignSolverConfig{
                .MaxMoveActionCount = 3
            }.MergeWith(GetOrCrash(bundle.Bundle->Config->Groups, group)->Parameterized),
            group,
            Logger),
        ToString(std::get<1>(params)));
}

INSTANTIATE_TEST_SUITE_P(
    TTestReassignTabletsParameterizedErrors,
    TTestReassignTabletsParameterizedErrors,
    ::testing::Values(
        std::make_tuple(
            TTestBundleParams{
                .Nodes = "[{node_address=home; used=0; limit=200}]",
                .Cells = "[{memory_size=70; node_address=home}; {memory_size=50}]",
                .BundleConfig = "{groups={default={parameterized={metric=\"double([/statistics/memory_size]\"}}}}",
                .Tables = std::vector<TTestTableParams>{
                    TTestTableParams{
                        .TableSettings = "{sorted=%true; in_memory_mode=uncompressed}",
                        .TableConfig = "{}",
                        .Tablets = std::vector<TStringBuf>{
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"},
                            TTestTabletParams{"{uncompressed_data_size=20; cell_index=0; memory_size=20}"},
                            TTestTabletParams{"{uncompressed_data_size=10; cell_index=1; memory_size=10}"},
                            TTestTabletParams{"{uncompressed_data_size=40; cell_index=1; memory_size=40}"}}}}},
            /*errorText*/ "Not all cells are assigned to nodes"),
        std::make_tuple(
            TTestBundleParams{
                .Nodes = "[{node_address=home; used=0; limit=100}]",
                .Cells = "[{memory_size=70; node_address=home}; {memory_size=50; node_address=home}]",
                .BundleConfig = "{groups={default={parameterized={metric=\"double([/statistics/memory_size])\"}}}}",
                .Tables = std::vector<TTestTableParams>{
                    TTestTableParams{
                        .TableSettings = "{sorted=%true; in_memory_mode=uncompressed}",
                        .TableConfig = "{}",
                        .Tablets = std::vector<TStringBuf>{
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"},
                            TTestTabletParams{"{uncompressed_data_size=20; cell_index=0; memory_size=20}"},
                            TTestTabletParams{"{uncompressed_data_size=10; cell_index=1; memory_size=10}"},
                            TTestTabletParams{"{uncompressed_data_size=40; cell_index=1; memory_size=40}"}}}}},
            /*errorText*/ "Node memory size limit is less than the actual cell memory size")));

////////////////////////////////////////////////////////////////////////////////

class TTestReassignTabletsParameterizedByNodes
    : public TTestReassignTabletsParameterized
{ };

TEST_P(TTestReassignTabletsParameterizedByNodes, SimpleManyNodesWithInMemoryTablets)
{
    const auto& params = GetParam();
    auto bundle = CreateTabletCellBundle(std::get<0>(params));
    const auto& table = GetOrCrash(bundle.Bundle->Tables, bundle.TableIds[0]);

    auto group = table->TableConfig->Group.value_or(DefaultGroupName);

    auto descriptors = ReassignTabletsParameterized(
        bundle.Bundle,
        /*performanceCountersKeys*/ {},
        TParameterizedReassignSolverConfig{
            .MaxMoveActionCount = std::get<2>(params)
        }.MergeWith(GetOrCrash(bundle.Bundle->Config->Groups, group)->Parameterized),
        group,
        Logger);

    auto expected = ConvertTo<std::vector<TTestMoveDescriptorPtr>>(TYsonStringBuf(std::get<1>(params)));

    EXPECT_EQ(std::ssize(expected), std::ssize(descriptors));

    THashMap<TTabletId, TTabletCellId> tabletToCell;
    for (const auto& cell : bundle.Cells) {
        for (const auto& [tabletId, tablet] : cell->Tablets) {
            EmplaceOrCrash(tabletToCell, tabletId, cell->Id);
        }
    }

    for (const auto& descriptor : descriptors) {
        auto tablet = FindTabletInBundle(bundle.Bundle, descriptor.TabletId);
        EXPECT_TRUE(tablet != nullptr);
        EXPECT_TRUE(tablet->Cell != nullptr);
        EXPECT_TRUE(tablet->Cell->Id != descriptor.TabletCellId);

        tabletToCell[descriptor.TabletId] = descriptor.TabletCellId;
    }

    THashMap<TTabletCellId, int> tabletCounts;
    for (auto [tabletId, cellId] : tabletToCell) {
        ++tabletCounts[cellId];
    }

    auto expectedDistribution = std::get<3>(params);

    std::vector<int> actualDistribution;
    for (const auto& cell : bundle.Cells) {
        if (auto it = tabletCounts.find(cell->Id); it != tabletCounts.end()) {
            actualDistribution.emplace_back(it->second);
        } else {
            actualDistribution.emplace_back(0);
        }
    }

    actualDistribution.resize(std::ssize(expectedDistribution));

    EXPECT_EQ(expectedDistribution, actualDistribution);

    THashMap<TTabletCellId, i64> cellToSize;
    for (auto [tabletId, cellId] : tabletToCell) {
        auto tablet = FindTabletInBundle(bundle.Bundle, tabletId);
        cellToSize[cellId] += tablet->Statistics.MemorySize;
    }

    for (const auto& cell : bundle.Cells) {
        cellToSize.emplace(cell->Id, 0);
    }

    auto expectedSizes = std::get<4>(params);

    std::vector<i64> cellSizes;
    for (const auto& cell : bundle.Cells) {
        cellSizes.emplace_back(GetOrCrash(cellToSize, cell->Id));
    }

    EXPECT_EQ(cellSizes, expectedSizes);

    THashMap<TString, i64> NodeMemoryUsed;
    for (const auto& [cellId, cell] : bundle.Bundle->TabletCells) {
        NodeMemoryUsed[*cell->NodeAddress] += GetOrCrash(cellToSize, cellId);
    }

    for (const auto& [node, statistics] : bundle.Bundle->NodeMemoryStatistics) {
        i64 used = 0;
        if (auto it = NodeMemoryUsed.find(node); it != NodeMemoryUsed.end()) {
            used = it->second;
        }

        if (statistics.Limit >= statistics.Used) {
            EXPECT_LE(used, statistics.Limit);
        } else {
            EXPECT_LE(used, statistics.Used);
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
    TTestReassignTabletsParameterizedByNodes,
    TTestReassignTabletsParameterizedByNodes,
    ::testing::Values(
        std::make_tuple(
            TTestBundleParams{
                .Nodes = "[{node_address=home1; used=120}; {node_address=home2; used=0}]",
                .Cells = "[{memory_size=70; node_address=home1}; {memory_size=50; node_address=home1}; {memory_size=0; node_address=home2}]",
                .BundleConfig = "{enable_parameterized_by_default=%true; groups={default={parameterized={metric=\"double([/statistics/memory_size])\"}}}}",
                .Tables = std::vector<TTestTableParams>{
                    TTestTableParams{
                        .TableSettings = "{sorted=%true; in_memory_mode=uncompressed}",
                        .TableConfig = "{}",
                        .Tablets = std::vector<TStringBuf>{
                            TTestTabletParams{"{uncompressed_data_size=50; cell_index=0; memory_size=50}"},
                            TTestTabletParams{"{uncompressed_data_size=20; cell_index=0; memory_size=20}"},
                            TTestTabletParams{"{uncompressed_data_size=10; cell_index=1; memory_size=10}"},
                            TTestTabletParams{"{uncompressed_data_size=40; cell_index=1; memory_size=40}"}}}}},
            /*moveDescriptors*/ "[{tablet_index=0; cell_index=2}; {tablet_index=2; cell_index=0}]",
            /*moveActionLimit*/ 2,
            /*distribution*/ std::vector<int>{2, 1, 1},
            /*cellSizes*/ std::vector<i64>{30, 40, 50}),
        std::make_tuple(
            TTestBundleParams{
                .Nodes = "[{node_address=home1; used=60; limit=60}; {node_address=home2; used=0; limit=5}]",
                .Cells = "[{memory_size=40; node_address=home1}; {memory_size=20; node_address=home1}; {memory_size=0; node_address=home2}]",
                .BundleConfig = "{enable_parameterized_by_default=%true; groups={default={parameterized={metric=\"double([/statistics/memory_size])\"}}}}",
                .Tables = std::vector<TTestTableParams>{
                    TTestTableParams{
                        .TableSettings = "{sorted=%true; in_memory_mode=uncompressed}",
                        .TableConfig = "{}",
                        .Tablets = std::vector<TStringBuf>{
                            TTestTabletParams{"{uncompressed_data_size=20; cell_index=0; memory_size=20}"},
                            TTestTabletParams{"{uncompressed_data_size=20; cell_index=0; memory_size=20}"},
                            TTestTabletParams{"{uncompressed_data_size=10; cell_index=1; memory_size=10}"},
                            TTestTabletParams{"{uncompressed_data_size=10; cell_index=1; memory_size=10}"}}}}},
            /*moveDescriptors*/ "[{tablet_index=0; cell_index=1}; {tablet_index=2; cell_index=0}]",
            /*moveActionLimit*/ 2,
            /*distribution*/ std::vector<int>{2, 2, 0},
            /*cellSizes*/ std::vector<i64>{30, 30, 0}),
        std::make_tuple(
            TTestBundleParams{
                .Nodes = "[{node_address=home1; used=40; limit=60}; {node_address=home2; used=20; limit=20}]",
                .Cells = "[{memory_size=40; node_address=home1}; {memory_size=20; node_address=home2}]",
                .BundleConfig = "{enable_parameterized_by_default=%true; groups={default={parameterized={metric=\"double([/statistics/memory_size])\"}}}}",
                .Tables = std::vector<TTestTableParams>{
                    TTestTableParams{
                        .TableSettings = "{sorted=%true; in_memory_mode=uncompressed}",
                        .TableConfig = "{}",
                        .Tablets = std::vector<TStringBuf>{
                            TTestTabletParams{"{uncompressed_data_size=20; cell_index=0; memory_size=20}"},
                            TTestTabletParams{"{uncompressed_data_size=20; cell_index=0; memory_size=20}"},
                            TTestTabletParams{"{uncompressed_data_size=10; cell_index=1; memory_size=10}"},
                            TTestTabletParams{"{uncompressed_data_size=10; cell_index=1; memory_size=10}"}}}}},
            /*moveDescriptors*/ "[]",
            /*moveActionLimit*/ 2,
            /*distribution*/ std::vector<int>{2, 2},
            /*cellSizes*/ std::vector<i64>{40, 20}),
        std::make_tuple(
            TTestBundleParams{
                .Nodes = "[{node_address=home1; used=50; limit=60}; {node_address=home2; used=10; limit=20}]",
                .Cells = "[{memory_size=50; node_address=home1}; {memory_size=10; node_address=home2}]",
                .BundleConfig = "{enable_parameterized_by_default=%true; groups={default={parameterized={metric=\"double([/statistics/memory_size])\"}}}}",
                .Tables = std::vector<TTestTableParams>{
                    TTestTableParams{
                        .TableSettings = "{sorted=%true; in_memory_mode=uncompressed}",
                        .TableConfig = "{}",
                        .Tablets = std::vector<TStringBuf>{
                            TTestTabletParams{"{uncompressed_data_size=20; cell_index=0; memory_size=21}"},
                            TTestTabletParams{"{uncompressed_data_size=20; cell_index=0; memory_size=19}"},
                            TTestTabletParams{"{uncompressed_data_size=10; cell_index=0; memory_size=10}"},
                            TTestTabletParams{"{uncompressed_data_size=10; cell_index=1; memory_size=10}"}}}}},
            /*moveDescriptors*/ "[{tablet_index=2; cell_index=1}]",
            /*moveActionLimit*/ 2,
            /*distribution*/ std::vector<int>{2, 2},
            /*cellSizes*/ std::vector<i64>{40, 20})));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletBalancer

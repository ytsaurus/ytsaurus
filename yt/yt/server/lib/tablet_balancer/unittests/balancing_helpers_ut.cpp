#include <yt/yt/server/lib/tablet_balancer/balancing_helpers.h>
#include <yt/yt/server/lib/tablet_balancer/config.h>
#include <yt/yt/server/lib/tablet_balancer/parameterized_balancing_helpers.h>
#include <yt/yt/server/lib/tablet_balancer/table.h>
#include <yt/yt/server/lib/tablet_balancer/tablet.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell_bundle.h>

#include <yt/yt/server/lib/tablet_balancer/dry_run/lib/helpers.h>
#include <yt/yt/server/lib/tablet_balancer/dry_run/lib/holders.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <random>

namespace NYT::NTabletBalancer {
namespace {

using namespace NDryRun;
using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("BalancingHelpersUnittest");

////////////////////////////////////////////////////////////////////////////////

TObjectId MakeObjectId(EObjectType type, int index)
{
    return MakeId(type, /*cellTag*/ MinValidCellTag, /*counter*/ 0, /*hash*/ index);
}

void FillObjectIdsInBundleHolder(const TBundleHolderPtr& bundle)
{
    for (const auto& cell : bundle->Cells) {
        YT_VERIFY(!cell->CellId);
        cell->CellId = MakeObjectId(EObjectType::TabletCell, cell->CellIndex);
    }

    for (const auto& table : bundle->Tables) {
        YT_VERIFY(!table->TableId);
        table->TableId  = MakeObjectId(EObjectType::Table, table->TableIndex);

        for (const auto& tablet : table->Tablets) {
            YT_VERIFY(!tablet->TabletId);
            tablet->TabletId = MakeObjectId(EObjectType::Tablet, tablet->TabletIndex);

            YT_VERIFY(!tablet->CellId);
            tablet->CellId = MakeObjectId(EObjectType::TabletCell, tablet->CellIndex);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TTestMoveDescriptor
    : public TYsonStruct
{
    int TabletIndex;
    int CellIndex;

    TMoveDescriptor CreateMoveDescriptor() const
    {
        return TMoveDescriptor{
            .TabletId = MakeObjectId(EObjectType::Tablet, TabletIndex),
            .TabletCellId = MakeObjectId(EObjectType::TabletCell, CellIndex)
        };
    }

    REGISTER_YSON_STRUCT(TTestMoveDescriptor);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("tablet_index", &TThis::TabletIndex)
            .Default(0);
        registrar.Parameter("cell_index", &TThis::CellIndex)
            .Default(0);
    }
};

DECLARE_REFCOUNTED_STRUCT(TTestMoveDescriptor)
DEFINE_REFCOUNTED_TYPE(TTestMoveDescriptor)

void CheckEqual(
    const TTestMoveDescriptorPtr& expected,
    const TMoveDescriptor& actual)
{
    auto expectedDescriptor = expected->CreateMoveDescriptor();
    EXPECT_EQ(actual.TabletId, expectedDescriptor.TabletId);
    EXPECT_EQ(actual.TabletCellId, expectedDescriptor.TabletCellId);
}

////////////////////////////////////////////////////////////////////////////////

struct TTestReshardDescriptor
    : public TYsonStruct
{
    std::vector<int> Tablets;
    i64 DataSize;
    int TabletCount;

    auto GetTabletIds() const
    {
        std::vector<TTabletId> tabletIds;
        for (const auto& tabletIndex : Tablets) {
            tabletIds.push_back(MakeObjectId(EObjectType::Tablet, tabletIndex));
        }
        return tabletIds;
    }

    REGISTER_YSON_STRUCT(TTestReshardDescriptor);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("tablets", &TThis::Tablets)
            .Default();
        registrar.Parameter("data_size", &TThis::DataSize)
            .Default(0);
        registrar.Parameter("tablet_count", &TThis::TabletCount)
            .Default(1);
    }
};

DECLARE_REFCOUNTED_STRUCT(TTestReshardDescriptor)
DEFINE_REFCOUNTED_TYPE(TTestReshardDescriptor)

void CheckEqual(
    const TTestReshardDescriptorPtr& expected,
    const TReshardDescriptor& actual)
{
    EXPECT_EQ(expected->DataSize, actual.DataSize);
    EXPECT_EQ(std::ssize(expected->Tablets), std::ssize(actual.Tablets));
    EXPECT_EQ(expected->TabletCount, actual.TabletCount);

    auto tabletIds = expected->GetTabletIds();
    for (int index = 0; index < std::ssize(actual.Tablets); ++index) {
        EXPECT_EQ(actual.Tablets[index], tabletIds[index]);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TTestTabletBalancingHelpers
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        /*bundle*/ TStringBuf,
        /*expectedDescriptors*/ TStringBuf>>
{ };

////////////////////////////////////////////////////////////////////////////////

class TTestReassignInMemoryTablets
    : public TTestTabletBalancingHelpers
{ };

TEST_P(TTestReassignInMemoryTablets, SimpleWithSameTablets)
{
    const auto& params = GetParam();
    auto bundleHolder = ConvertTo<TBundleHolderPtr>(TYsonStringBuf(std::get<0>(params)));
    FillObjectIdsInBundleHolder(bundleHolder);
    auto bundle = bundleHolder->CreateBundle();

    auto descriptors = ReassignInMemoryTablets(
        bundle,
        /*movableTables*/ std::nullopt,
        /*ignoreTableWiseConfig*/ false,
        Logger);

    auto expected = ConvertTo<std::vector<TTestMoveDescriptorPtr>>(TYsonStringBuf(std::get<1>(params)));
    EXPECT_EQ(std::ssize(expected), std::ssize(descriptors));

    std::sort(descriptors.begin(), descriptors.end(), [] (const TMoveDescriptor& lhs, const TMoveDescriptor& rhs) {
        return lhs.TabletId < rhs.TabletId;
    });

    for (int index = 0; index < std::ssize(expected); ++index) {
        CheckEqual(expected[index], descriptors[index]);
    }
}

INSTANTIATE_TEST_SUITE_P(
    TTestReassignInMemoryTablets,
    TTestReassignInMemoryTablets,
    ::testing::Values(
        std::tuple(
            "{tables=[{in_memory_mode=uncompressed; tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=50; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=50; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=100; node_address=home};"
                   "{cell_index=2; memory_size=0; node_address=home}];"
            "nodes=[{node_address=home; memory_used=100}]}",
            /*moveDescriptors*/ "[{tablet_index=1; cell_index=2};]"),
        std::tuple(
            "{tables=[{in_memory_mode=none; tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=50; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=50; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=100; node_address=home};"
                   "{cell_index=2; memory_size=0; node_address=home}];"
            "nodes=[{node_address=home; memory_used=100}]}",
            /*moveDescriptors*/ "[]"),
        std::tuple(
            "{tables=[{in_memory_mode=uncompressed; tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=50; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=50; partition_count=1}};"
            "{tablet_index=3; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=50; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=150; node_address=home};"
                   "{cell_index=2; memory_size=0; node_address=home};"
                   "{cell_index=3; memory_size=0; node_address=home}];"
            "nodes=[{node_address=home; memory_used=150}]}",
            /*moveDescriptors*/ "[{tablet_index=1; cell_index=2};"
                "{tablet_index=2; cell_index=3}]")));

////////////////////////////////////////////////////////////////////////////////

class TTestReassignOrdinaryTablets
    : public TTestTabletBalancingHelpers
{ };

TEST_P(TTestReassignOrdinaryTablets, Simple)
{
    const auto& params = GetParam();
    auto bundleHolder = ConvertTo<TBundleHolderPtr>(TYsonStringBuf(std::get<0>(params)));
    FillObjectIdsInBundleHolder(bundleHolder);
    auto bundle = bundleHolder->CreateBundle();

    auto descriptors = ReassignOrdinaryTablets(
        bundle,
        /*movableTables*/ std::nullopt,
        Logger);

    auto expected = ConvertTo<std::vector<TTestMoveDescriptorPtr>>(TYsonStringBuf(std::get<1>(params)));
    EXPECT_EQ(std::ssize(expected), std::ssize(descriptors));

    EXPECT_EQ(std::ssize(bundle->Tables), 1);

    auto table = bundle->Tables.begin()->second;

    const auto& anyTablet = table->Tablets[0];
    const auto& anyTabletCell = anyTablet->Cell.Lock();
    THashMap<TTabletCellId, THashSet<TTabletId>> cellToTablets;
    for (const auto& [id, cell] : bundle->TabletCells) {
        EmplaceOrCrash(cellToTablets, id, THashSet<TTabletId>{});
    }

    for (const auto& tablet : table->Tablets) {
        EXPECT_EQ(tablet->Statistics.MemorySize, anyTablet->Statistics.MemorySize);
        EXPECT_EQ(tablet->Statistics.UncompressedDataSize, anyTablet->Statistics.UncompressedDataSize);
        auto cell = tablet->Cell.Lock();
        EXPECT_EQ(cell, anyTabletCell);
        InsertOrCrash(cellToTablets[cell->Id], tablet->Id);
    }

    auto expectedCellToTablets = cellToTablets;
    for (const auto& descriptor : descriptors) {
        auto tablet = FindTabletInBundle(bundle, descriptor.TabletId);
        YT_VERIFY(tablet);

        InsertOrCrash(cellToTablets[descriptor.TabletCellId], descriptor.TabletId);
        EraseOrCrash(cellToTablets[tablet->Cell.Lock()->Id], descriptor.TabletId);
    }

    for (const auto& descriptor : expected) {
        auto expectedDescriptor = descriptor->CreateMoveDescriptor();

        auto tablet = FindTabletInBundle(bundle, expectedDescriptor.TabletId);

        InsertOrCrash(expectedCellToTablets[expectedDescriptor.TabletCellId], expectedDescriptor.TabletId);
        EraseOrCrash(expectedCellToTablets[tablet->Cell.Lock()->Id], expectedDescriptor.TabletId);
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
        std::tuple(
            "{tables=[{in_memory_mode=uncompressed; tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=50; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=50; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=100; node_address=home};"
                   "{cell_index=2; memory_size=0; node_address=home}];"
            "nodes=[{node_address=home; memory_used=100}]}",
            /*moveDescriptors*/ "[]"),
        std::tuple(
            "{tables=[{in_memory_mode=none; tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=50; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=50; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=100; node_address=home};"
                   "{cell_index=2; memory_size=0; node_address=home}];"
            "nodes=[{node_address=home; memory_used=100}]}",
            /*moveDescriptors*/ "[{tablet_index=2; cell_index=2}]"),
        std::tuple(
            "{tables=[{in_memory_mode=none; tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=50; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=50; partition_count=1}};"
            "{tablet_index=3; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=50; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=150; node_address=home};"
                   "{cell_index=2; memory_size=0; node_address=home};"
                   "{cell_index=3; memory_size=0; node_address=home}];"
            "nodes=[{node_address=home; memory_used=150}]}",
            /*moveDescriptors*/ "[{tablet_index=2; cell_index=2};"
                "{tablet_index=3; cell_index=3}]")
        ));

// ////////////////////////////////////////////////////////////////////////////////

class TTestMergeSplitTabletsOfTable
    : public TTestTabletBalancingHelpers
{ };

TEST_P(TTestMergeSplitTabletsOfTable, Simple)
{
    const auto& params = GetParam();
    auto bundleHolder = ConvertTo<TBundleHolderPtr>(TYsonStringBuf(std::get<0>(params)));
    FillObjectIdsInBundleHolder(bundleHolder);
    auto bundle = bundleHolder->CreateBundle();

    auto expected = ConvertTo<std::vector<TTestReshardDescriptorPtr>>(TYsonStringBuf(std::get<1>(params)));
    auto expectedDescriptorsIt = expected.begin();

    for (const auto& [id, table] : bundle->Tables) {
        YT_VERIFY(table->CompressedDataSize);
        YT_VERIFY(table->UncompressedDataSize);

        auto descriptors = MergeSplitTabletsOfTable(
            table->Tablets,
            /*minDesiredTabletSize*/ 0,
            /*pickPivotKeys*/ true,
            Logger);

        for (const auto& descriptor : descriptors) {
            EXPECT_NE(expectedDescriptorsIt, expected.end());
            CheckEqual(*expectedDescriptorsIt, descriptor);
            ++expectedDescriptorsIt;
        }
    }
    EXPECT_EQ(expectedDescriptorsIt, expected.end());
}

INSTANTIATE_TEST_SUITE_P(
    TTestMergeSplitTabletsOfTable,
    TTestMergeSplitTabletsOfTable,
    ::testing::Values(
        std::tuple(
            "{tables=[{in_memory_mode=none; uncompressed_data_size=100; compressed_data_size=100;"
                      "config={desired_tablet_count=100};"
                      "tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=100; memory_size=100; compressed_data_size=100; partition_count=2}}]}];"
            "cells=[{cell_index=1; memory_size=100}]}",
            /*reshardDescriptors*/ "[{tablets=[1;]; tablet_count=100; data_size=100}]"),
        std::tuple(
            "{config={min_tablet_size=200};"
            "tables=[{in_memory_mode=none; uncompressed_data_size=300; compressed_data_size=300; tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=100; memory_size=100; compressed_data_size=100; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=100; memory_size=100; compressed_data_size=100; partition_count=1}};"
            "{tablet_index=3; cell_index=1;"
                "statistics={uncompressed_data_size=100; memory_size=100; compressed_data_size=100; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=300}]}",
            /*reshardDescriptors*/ "[{tablets=[1;2;]; tablet_count=1; data_size=200}]")
        ));

// ////////////////////////////////////////////////////////////////////////////////

class TTestTabletBalancingHelpersWithoutDescriptors
    : public ::testing::Test
    , public ::testing::WithParamInterface<TStringBuf>
{ };

////////////////////////////////////////////////////////////////////////////////

class TTestReassignInMemoryTabletsUniform
    : public TTestTabletBalancingHelpersWithoutDescriptors
{ };

TEST_P(TTestReassignInMemoryTabletsUniform, Simple)
{
    const auto& params = GetParam();
    auto bundleHolder = ConvertTo<TBundleHolderPtr>(TYsonStringBuf(params));
    FillObjectIdsInBundleHolder(bundleHolder);
    auto bundle = bundleHolder->CreateBundle();

    auto descriptors = ReassignInMemoryTablets(
        bundle,
        /*movableTables*/ std::nullopt,
        /*ignoreTableWiseConfig*/ false,
        Logger);

    i64 totalSize = 0;
    THashMap<TTabletCellId, i64> cellSizes;
    for (const auto& [id, cell] : bundle->TabletCells) {
        EmplaceOrCrash(cellSizes, id, cell->Statistics.MemorySize);
        totalSize += cell->Statistics.MemorySize;
    }

    for (const auto& descriptor : descriptors) {
        auto tablet = FindTabletInBundle(bundle, descriptor.TabletId);
        EXPECT_TRUE(tablet != nullptr);
        auto cell = tablet->Cell.Lock();
        EXPECT_TRUE(cell != nullptr);
        EXPECT_TRUE(cell->Id != descriptor.TabletCellId);
        EXPECT_TRUE(tablet->Table->InMemoryMode != EInMemoryMode::None);
        auto tabletSize = GetTabletBalancingSize(tablet.Get());
        cellSizes[cell->Id] -= tabletSize;
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
    TTestReassignInMemoryTabletsUniform,
    TTestReassignInMemoryTabletsUniform,
    ::testing::Values(
        "{tables=[{in_memory_mode=uncompressed; tablets=["
        "{tablet_index=1; cell_index=1;"
            "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=50; partition_count=1}};"
        "{tablet_index=2; cell_index=1;"
            "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=50; partition_count=1}}]}];"
        "cells=[{cell_index=1; memory_size=100; node_address=home};"
                "{cell_index=2; memory_size=0; node_address=home}];"
        "nodes=[{node_address=home; memory_used=100}]}",

        "{tables=[{in_memory_mode=uncompressed; tablets=["
        "{tablet_index=1; cell_index=1;"
            "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=50; partition_count=1}};"
        "{tablet_index=2; cell_index=1;"
            "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=50; partition_count=1}};"
        "{tablet_index=3; cell_index=1;"
            "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=50; partition_count=1}}]}];"
        "cells=[{cell_index=1; memory_size=150; node_address=home};"
                "{cell_index=2; memory_size=0; node_address=home};"
                "{cell_index=3; memory_size=0; node_address=home}];"
        "nodes=[{node_address=home; memory_used=150}]}"
    ));

////////////////////////////////////////////////////////////////////////////////

class TTestReassignOrdinaryTabletsUniform
    : public TTestTabletBalancingHelpersWithoutDescriptors
{ };

TEST_P(TTestReassignOrdinaryTabletsUniform, Simple)
{
    const auto& params = GetParam();
    auto bundleHolder = ConvertTo<TBundleHolderPtr>(TYsonStringBuf(params));
    FillObjectIdsInBundleHolder(bundleHolder);
    auto bundle = bundleHolder->CreateBundle();

    auto descriptors = ReassignOrdinaryTablets(
        bundle,
        /*movableTables*/ std::nullopt,
        Logger);

    i64 totalSize = 0;
    THashMap<TTabletCellId, i64> cellSizes;
    for (const auto& [id, cell] : bundle->TabletCells) {
        EmplaceOrCrash(cellSizes, id, cell->Statistics.MemorySize);
        totalSize += cell->Statistics.MemorySize;
    }

    for (const auto& descriptor : descriptors) {
        auto tablet = FindTabletInBundle(bundle, descriptor.TabletId);
        EXPECT_TRUE(tablet != nullptr);
        auto cell = tablet->Cell.Lock();
        EXPECT_TRUE(cell != nullptr);
        EXPECT_TRUE(cell->Id != descriptor.TabletCellId);
        EXPECT_TRUE(tablet->Table->InMemoryMode == EInMemoryMode::None);
        auto tabletSize = GetTabletBalancingSize(tablet.Get());
        cellSizes[cell->Id] -= tabletSize;
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
        "{tables=[{in_memory_mode=none; tablets=["
        "{tablet_index=1; cell_index=1;"
            "statistics={uncompressed_data_size=50; memory_size=0; compressed_data_size=50; partition_count=1}};"
        "{tablet_index=2; cell_index=1;"
            "statistics={uncompressed_data_size=50; memory_size=0; compressed_data_size=50; partition_count=1}}]}];"
        "cells=[{cell_index=1; memory_size=100; node_address=home};"
                "{cell_index=2; memory_size=0; node_address=home}];"
        "nodes=[{node_address=home; memory_used=100}]}",

        "{tables=[{in_memory_mode=none; tablets=["
        "{tablet_index=1; cell_index=1;"
            "statistics={uncompressed_data_size=50; memory_size=0; compressed_data_size=50; partition_count=1}};"
        "{tablet_index=2; cell_index=1;"
            "statistics={uncompressed_data_size=50; memory_size=0; compressed_data_size=50; partition_count=1}};"
        "{tablet_index=3; cell_index=1;"
            "statistics={uncompressed_data_size=50; memory_size=0; compressed_data_size=50; partition_count=1}}]}];"
        "cells=[{cell_index=1; memory_size=150; node_address=home};"
                "{cell_index=2; memory_size=0; node_address=home};"
                "{cell_index=3; memory_size=0; node_address=home}];"
        "nodes=[{node_address=home; memory_used=150}]}"
    ));

////////////////////////////////////////////////////////////////////////////////

class TTestReassignTabletsParameterized
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        /*bundle*/ TStringBuf,
        /*expectedDescriptors*/ TStringBuf,
        /*moveActionLimit*/ int,
        /*distribution*/ std::vector<int>,
        /*cellSizes*/ std::vector<i64>>>
{ };

TEST_P(TTestReassignTabletsParameterized, ViaMemorySize)
{
    const auto& params = GetParam();
    auto bundleHolder = ConvertTo<TBundleHolderPtr>(TYsonStringBuf(std::get<0>(params)));
    FillObjectIdsInBundleHolder(bundleHolder);
    auto bundle = bundleHolder->CreateBundle();

    const auto& table = bundle->Tables.begin()->second;
    auto group = table->TableConfig->Group.value_or(DefaultGroupName);

    auto descriptors = ReassignTabletsParameterized(
        bundle,
        /*performanceCountersKeys*/ {},
        /*performanceCountersTableSchema*/ nullptr,
        TParameterizedReassignSolverConfig{
            .MaxMoveActionCount = std::get<2>(params)
        }.MergeWith(GetOrCrash(bundle->Config->Groups, group)->Parameterized),
        group,
        /*metricTracker*/ nullptr,
        Logger);

    auto expected = ConvertTo<std::vector<TTestMoveDescriptorPtr>>(TYsonStringBuf(std::get<1>(params)));
    EXPECT_EQ(std::ssize(expected), std::ssize(descriptors));

    THashMap<TTabletId, TTabletCellId> tabletToCell;
    for (const auto& [id, cell] : bundle->TabletCells) {
        for (const auto& [tabletId, tablet] : cell->Tablets) {
            EmplaceOrCrash(tabletToCell, tabletId, id);
        }
    }

    for (int index = 0; index < std::ssize(descriptors); ++index) {
        auto tablet = FindTabletInBundle(bundle, descriptors[index].TabletId);
        EXPECT_TRUE(tablet != nullptr);
        auto cell = tablet->Cell.Lock();
        EXPECT_TRUE(cell != nullptr);
        EXPECT_TRUE(cell->Id != descriptors[index].TabletCellId);

        tabletToCell[descriptors[index].TabletId] = descriptors[index].TabletCellId;
        CheckEqual(expected[index], descriptors[index]);
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
        auto tablet = FindTabletInBundle(bundle, tabletId);
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
        std::tuple( // NO ACTIONS
            "{config={groups={rex={parameterized={metric=\"int64([/statistics/memory_size]) + int64([/statistics/uncompressed_data_size])\"}}}};"
            "tables=[{in_memory_mode=uncompressed;"
                     "config={enable_parameterized=%true; group=rex};"
                     "tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=50; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=50; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=100; node_address=home};"
                   "{cell_index=2; memory_size=0; node_address=home}];"
            "nodes=[{node_address=home; memory_used=100}]}",
            /*moveDescriptors*/ "[]",
            /*moveActionLimit*/ 0,
            /*distribution*/ std::vector<int>{2, 0},
            /*cellSizes*/ std::vector<i64>{100, 0}),
        std::tuple( // MOVE
            "{config={groups={default={parameterized={metric=\"double([/statistics/memory_size])\"}}}};"
            "tables=[{in_memory_mode=uncompressed;"
                     "config={enable_parameterized=%true};"
                     "tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=60; memory_size=60; compressed_data_size=60; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=40; memory_size=40; compressed_data_size=40; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=100; node_address=home};"
                   "{cell_index=2; memory_size=0; node_address=home}];"
            "nodes=[{node_address=home; memory_used=100; memory_limit=100}]}",
            /*moveDescriptors*/ "[{tablet_index=2; cell_index=2};]",
            /*moveActionLimit*/ 1,
            /*distribution*/ std::vector<int>{1, 1},
            /*cellSizes*/ std::vector<i64>{60, 40}),
        std::tuple( // MOVE (group)
            "{config={groups={rex={parameterized={metric=\"0\"}};"
                "fex={parameterized={metric=\"int64([/statistics/memory_size]) + int64([/statistics/uncompressed_data_size])\"}}}};"
            "tables=[{in_memory_mode=uncompressed;"
                     "config={group=fex; enable_parameterized=%true};"
                     "tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=60; memory_size=60; compressed_data_size=60; partition_count=1}};"
            "{tablet_index=2; cell_index=2;"
                "statistics={uncompressed_data_size=5; memory_size=5; compressed_data_size=5; partition_count=1}};"
            "{tablet_index=3; cell_index=1;"
                "statistics={uncompressed_data_size=40; memory_size=40; compressed_data_size=40; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=100; node_address=home};"
                   "{cell_index=2; memory_size=5; node_address=home}];"
            "nodes=[{node_address=home; memory_used=0; memory_limit=200}]}",
            /*moveDescriptors*/ "[{tablet_index=3; cell_index=2};]",
            /*moveActionLimit*/ 1,
            /*distribution*/ std::vector<int>{1, 2},
            /*cellSizes*/ std::vector<i64>{60, 45}),
        std::tuple( // MOVE (available action count is more than needed)
            "{config={enable_parameterized_by_default=%true; groups={default={parameterized={metric=\"double([/statistics/memory_size])\"}}}};"
            "tables=[{in_memory_mode=uncompressed; tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=10; memory_size=10; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=3; cell_index=2;"
                "statistics={uncompressed_data_size=5; memory_size=5; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=4; cell_index=2;"
                "statistics={uncompressed_data_size=40; memory_size=40; compressed_data_size=0; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=70; node_address=home};"
                   "{cell_index=2; memory_size=50; node_address=home}];"
            "nodes=[{node_address=home; memory_used=0; memory_limit=200}]}",
            /*moveDescriptors*/ "[{tablet_index=2; cell_index=2};]",
            /*moveActionLimit*/ 3,
            /*distribution*/ std::vector<int>{1, 3},
            /*cellSizes*/ std::vector<i64>{50, 55}),
        std::tuple( // DISABLE BALANCING
            "{config={groups={default={parameterized={metric=\"double([/statistics/memory_size])\"}}}};"
            "tables=[{in_memory_mode=uncompressed; tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=0; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=100; node_address=home};"
                   "{cell_index=2; memory_size=0; node_address=home}];"
            "nodes=[{node_address=home; memory_used=100; memory_limit=200}]}",
            /*moveDescriptors*/ "[]",
            /*moveActionLimit*/ 3,
            /*distribution*/ std::vector<int>{2, 0},
            /*cellSizes*/ std::vector<i64>{100, 0}),
        std::tuple( // DISABLE BALANCING HARD
            "{config={groups={default={parameterized={metric=\"1\"}}}};"
            "tables=[{in_memory_mode=uncompressed;"
                     "config={enable_parameterized=%false};"
                     "tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=0; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=100; node_address=home};"
                   "{cell_index=2; memory_size=0; node_address=home}];"
            "nodes=[{node_address=home; memory_used=100; memory_limit=200}]}",
            /*moveDescriptors*/ "[]",
            /*moveActionLimit*/ 3,
            /*distribution*/ std::vector<int>{2, 0},
            /*cellSizes*/ std::vector<i64>{100, 0})));

////////////////////////////////////////////////////////////////////////////////

class TTestReassignTabletsParameterizedErrors
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        /*bundle*/ TStringBuf,
        /*errorText*/ TString>>
{ };

TEST_P(TTestReassignTabletsParameterizedErrors, BalancingError)
{
    const auto& params = GetParam();
    auto bundleHolder = ConvertTo<TBundleHolderPtr>(TYsonStringBuf(std::get<0>(params)));
    FillObjectIdsInBundleHolder(bundleHolder);
    auto bundle = bundleHolder->CreateBundle();

    const auto& table = bundle->Tables.begin()->second;
    auto group = table->TableConfig->Group.value_or(DefaultGroupName);

    EXPECT_THROW_WITH_SUBSTRING(
        ReassignTabletsParameterized(
            bundle,
            /*performanceCountersKeys*/ {},
            /*performanceCountersTableSchema*/ nullptr,
            TParameterizedReassignSolverConfig{
                .MaxMoveActionCount = 3
            }.MergeWith(GetOrCrash(bundle->Config->Groups, group)->Parameterized),
            group,
            /*metricTracker*/ nullptr,
            Logger),
        ToString(std::get<1>(params)));
}

INSTANTIATE_TEST_SUITE_P(
    TTestReassignTabletsParameterizedErrors,
    TTestReassignTabletsParameterizedErrors,
    ::testing::Values(
        std::tuple(
            "{config={groups={default={parameterized={metric=\"double([/statistics/memory_size])\"}}}};"
            "tables=[{in_memory_mode=uncompressed; tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=20; memory_size=20; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=3; cell_index=2;"
                "statistics={uncompressed_data_size=10; memory_size=10; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=4; cell_index=2;"
                "statistics={uncompressed_data_size=40; memory_size=40; compressed_data_size=0; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=70; node_address=home};"
                   "{cell_index=2; memory_size=50; node_address=home}];"
            "nodes=[{node_address=home; memory_used=0; memory_limit=100}]}",
            /*errorText*/ "Node memory usage exceeds memory limit")));

////////////////////////////////////////////////////////////////////////////////

class TTestReassignTabletsParameterizedByNodes
    : public TTestReassignTabletsParameterized
{ };

TEST_P(TTestReassignTabletsParameterizedByNodes, ManyNodesWithInMemoryTablets)
{
    const auto& params = GetParam();
    auto bundleHolder = ConvertTo<TBundleHolderPtr>(TYsonStringBuf(std::get<0>(params)));
    FillObjectIdsInBundleHolder(bundleHolder);
    auto bundle = bundleHolder->CreateBundle();

    const auto& table = bundle->Tables.begin()->second;
    auto group = table->TableConfig->Group.value_or(DefaultGroupName);

    auto descriptors = ReassignTabletsParameterized(
        bundle,
        /*performanceCountersKeys*/ {},
        /*performanceCountersTableSchema*/ nullptr,
        TParameterizedReassignSolverConfig{
            .MaxMoveActionCount = std::get<2>(params)
        }.MergeWith(GetOrCrash(bundle->Config->Groups, group)->Parameterized),
        group,
        /*metricTracker*/ nullptr,
        Logger);

    auto expected = ConvertTo<std::vector<TTestMoveDescriptorPtr>>(TYsonStringBuf(std::get<1>(params)));

    EXPECT_EQ(std::ssize(expected), std::ssize(descriptors));

    THashMap<TTabletId, TTabletCellId> tabletToCell;
    for (const auto& [id, cell] : bundle->TabletCells) {
        for (const auto& [tabletId, tablet] : cell->Tablets) {
            EmplaceOrCrash(tabletToCell, tabletId, cell->Id);
        }
    }

    for (int index = 0; index < std::ssize(descriptors); ++index) {
        auto tablet = FindTabletInBundle(bundle, descriptors[index].TabletId);
        EXPECT_TRUE(tablet != nullptr);
        auto cell = tablet->Cell.Lock();
        EXPECT_TRUE(cell != nullptr);
        EXPECT_TRUE(cell->Id != descriptors[index].TabletCellId);

        tabletToCell[descriptors[index].TabletId] = descriptors[index].TabletCellId;
        CheckEqual(expected[index], descriptors[index]);
    }

    THashMap<TTabletCellId, int> tabletCounts;
    for (auto [tabletId, cellId] : tabletToCell) {
        ++tabletCounts[cellId];
    }

    auto expectedDistribution = std::get<3>(params);

    std::vector<int> actualDistribution;
    for (const auto& [id, cell] : bundle->TabletCells) {
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
        auto tablet = FindTabletInBundle(bundle, tabletId);
        cellToSize[cellId] += tablet->Statistics.MemorySize;
    }

    for (const auto& [id, cell] : bundle->TabletCells) {
        cellToSize.emplace(cell->Id, 0);
    }

    auto expectedSizes = std::get<4>(params);

    std::vector<i64> cellSizes;
    for (const auto& [id, cell] : bundle->TabletCells) {
        cellSizes.emplace_back(GetOrCrash(cellToSize, cell->Id));
    }

    EXPECT_EQ(cellSizes, expectedSizes);

    THashMap<TString, i64> NodeMemoryUsed;
    for (const auto& [cellId, cell] : bundle->TabletCells) {
        if (!cell->NodeAddress.has_value()) {
            continue;
        }

        NodeMemoryUsed[*cell->NodeAddress] += GetOrCrash(cellToSize, cellId);
    }

    for (const auto& [node, statistics] : bundle->NodeStatistics) {
        i64 used = 0;
        if (auto it = NodeMemoryUsed.find(node); it != NodeMemoryUsed.end()) {
            used = it->second;
        }

        if (statistics.MemoryLimit >= statistics.MemoryUsed) {
            EXPECT_LE(used, statistics.MemoryLimit);
        } else {
            EXPECT_LE(used, statistics.MemoryUsed);
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
    TTestReassignTabletsParameterizedByNodes,
    TTestReassignTabletsParameterizedByNodes,
    ::testing::Values(
        std::tuple(
            "{config={enable_parameterized_by_default=%true; groups={default={parameterized={metric=\"double([/statistics/memory_size])\"}}}};"
            "tables=[{in_memory_mode=uncompressed; tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=20; memory_size=20; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=3; cell_index=2;"
                "statistics={uncompressed_data_size=10; memory_size=10; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=4; cell_index=2;"
                "statistics={uncompressed_data_size=40; memory_size=40; compressed_data_size=0; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=70; node_address=home1};"
                   "{cell_index=2; memory_size=50; node_address=home1};"
                   "{cell_index=3; memory_size=0; node_address=home2}];"
            "nodes=[{node_address=home1; memory_used=120; tablet_slot_count=2};"
                   "{node_address=home2; memory_used=0}]}",
            /*moveDescriptors*/ "[{tablet_index=1; cell_index=3}; {tablet_index=3; cell_index=1}]",
            /*moveActionLimit*/ 2,
            /*distribution*/ std::vector<int>{2, 1, 1},
            /*cellSizes*/ std::vector<i64>{30, 40, 50}),
        std::tuple(
            "{config={enable_parameterized_by_default=%true; groups={default={parameterized={metric=\"double([/statistics/memory_size])\"}}}};"
            "tables=[{in_memory_mode=uncompressed; tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=20; memory_size=20; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=20; memory_size=20; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=3; cell_index=2;"
                "statistics={uncompressed_data_size=10; memory_size=10; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=4; cell_index=2;"
                "statistics={uncompressed_data_size=10; memory_size=10; compressed_data_size=0; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=40; node_address=home1};"
                   "{cell_index=2; memory_size=20; node_address=home1};"
                   "{cell_index=3; memory_size=0; node_address=home2}];"
            "nodes=[{node_address=home1; memory_used=60; memory_limit=60; tablet_slot_count=2};"
                   "{node_address=home2; memory_used=0; memory_limit=5}]}",
            /*moveDescriptors*/ "[]",
            /*moveActionLimit*/ 2,
            /*distribution*/ std::vector<int>{2, 2, 0},
            /*cellSizes*/ std::vector<i64>{40, 20, 0}),
        std::tuple(
            "{config={enable_parameterized_by_default=%true; groups={default={parameterized={metric=\"double([/statistics/memory_size])\"}}}};"
            "tables=[{in_memory_mode=uncompressed; tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=20; memory_size=20; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=20; memory_size=20; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=3; cell_index=2;"
                "statistics={uncompressed_data_size=10; memory_size=10; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=4; cell_index=2;"
                "statistics={uncompressed_data_size=10; memory_size=10; compressed_data_size=0; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=40; node_address=home1};"
                   "{cell_index=2; memory_size=20; node_address=home2}];"
            "nodes=[{node_address=home1; memory_used=40; memory_limit=60};"
                   "{node_address=home2; memory_used=20; memory_limit=20}]}",
            /*moveDescriptors*/ "[]",
            /*moveActionLimit*/ 2,
            /*distribution*/ std::vector<int>{2, 2},
            /*cellSizes*/ std::vector<i64>{40, 20}),
        std::tuple(
            "{config={enable_parameterized_by_default=%true; groups={default={parameterized={metric=\"double([/statistics/memory_size])\"}}}};"
            "tables=[{in_memory_mode=uncompressed; tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=20; memory_size=20; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=20; memory_size=20; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=3; cell_index=2;"
                "statistics={uncompressed_data_size=10; memory_size=10; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=4; cell_index=3;"
                "statistics={uncompressed_data_size=10; memory_size=10; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=5; cell_index=2;"
                "statistics={uncompressed_data_size=10; memory_size=10; compressed_data_size=0; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=40; node_address=home1};"
                   "{cell_index=2; memory_size=20; node_address=home1};"
                   "{cell_index=3; memory_size=10};"
                   "{cell_index=4; memory_size=0; node_address=home2}];"
            "nodes=[{node_address=home1; memory_used=60; memory_limit=60; tablet_slot_count=2};"
                   "{node_address=home2; memory_used=0; memory_limit=0}]}",
            /*moveDescriptors*/ "[]",
            /*moveActionLimit*/ 2,
            /*distribution*/ std::vector<int>{2, 2, 1, 0},
            /*cellSizes*/ std::vector<i64>{40, 20, 10, 0}),
        std::tuple(
            "{config={enable_parameterized_by_default=%true; groups={default={parameterized={metric=\"double([/statistics/memory_size])\"}}}};"
            "tables=[{in_memory_mode=uncompressed; tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=20; memory_size=21; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=20; memory_size=19; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=3; cell_index=1;"
                "statistics={uncompressed_data_size=10; memory_size=10; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=4; cell_index=2;"
                "statistics={uncompressed_data_size=10; memory_size=10; compressed_data_size=0; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=50; node_address=home1};"
                   "{cell_index=2; memory_size=10; node_address=home2}];"
            "nodes=[{node_address=home1; memory_used=50; memory_limit=60};"
                   "{node_address=home2; memory_used=10; memory_limit=20}]}",
            /*moveDescriptors*/ "[{tablet_index=3; cell_index=2}]",
            /*moveActionLimit*/ 2,
            /*distribution*/ std::vector<int>{2, 2},
            /*cellSizes*/ std::vector<i64>{40, 20})));

////////////////////////////////////////////////////////////////////////////////

class TTestMergeSplitTabletsParameterized
    : public TTestTabletBalancingHelpers
{ };

TEST_P(TTestMergeSplitTabletsParameterized, ViaMemorySize)
{
    const auto& params = GetParam();
    auto bundleHolder = ConvertTo<TBundleHolderPtr>(TYsonStringBuf(std::get<0>(params)));
    FillObjectIdsInBundleHolder(bundleHolder);
    auto bundle = bundleHolder->CreateBundle();

    const auto& table = bundle->Tables.begin()->second;
    auto group = table->TableConfig->Group.value_or(DefaultGroupName);

    auto resharder = CreateParameterizedResharder(
        bundle,
        /*performanceCountersKeys*/ {},
        /*performanceCountersTableSchema*/ nullptr,
        TParameterizedResharderConfig{}.MergeWith(GetOrCrash(bundle->Config->Groups, group)->Parameterized),
        group,
        Logger);

    auto expected = ConvertTo<std::vector<TTestReshardDescriptorPtr>>(TYsonStringBuf(std::get<1>(params)));

    auto expectedDescriptorsIt = expected.begin();

    for (const auto& [id, table] : bundle->Tables) {
        YT_VERIFY(table->TableConfig->DesiredTabletCount);

        auto descriptors = resharder->BuildTableActionDescriptors(table);
        for (const auto& descriptor : descriptors) {
            EXPECT_NE(expectedDescriptorsIt, expected.end());
            CheckEqual(*expectedDescriptorsIt, descriptor);
            ++expectedDescriptorsIt;
        }
    }
    EXPECT_EQ(expectedDescriptorsIt, expected.end());
}

INSTANTIATE_TEST_SUITE_P(
    TTestMergeSplitTabletsParameterized,
    TTestMergeSplitTabletsParameterized,
    ::testing::Values(
        std::tuple( // SPLIT
            "{config={groups={default={parameterized={metric=\"double([/statistics/memory_size])\"; enable_reshard=%true}}}};"
            "tables=[{in_memory_mode=uncompressed; uncompressed_data_size=200; compressed_data_size=200;"
                     "config={enable_parameterized=%true; desired_tablet_count=2};"
                     "tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=200; memory_size=200; compressed_data_size=200; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=200; node_address=home}];"
            "nodes=[{node_address=home; memory_used=200}]}",
            /*reshardDescriptors*/ "[{tablets=[1;]; tablet_count=2; data_size=200}]"),
        std::tuple( // SPLIT (by size)
            "{config={groups={default={parameterized={metric=\"double([/statistics/memory_size])\"; enable_reshard=%true}}}};"
            "tables=[{in_memory_mode=uncompressed; uncompressed_data_size=200; compressed_data_size=200;"
                     "config={enable_parameterized=%true; desired_tablet_count=2};"
                     "tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=200; memory_size=0; compressed_data_size=100; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=1; memory_size=0; compressed_data_size=100; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=1; node_address=home}];"
            "nodes=[{node_address=home; memory_used=1}]}",
            /*reshardDescriptors*/ "[{tablets=[1;]; tablet_count=2; data_size=200}]"),
        std::tuple( // SPLIT (by size, swapped)
            "{config={groups={default={parameterized={metric=\"double([/statistics/memory_size])\"; enable_reshard=%true}}}};"
            "tables=[{in_memory_mode=uncompressed; uncompressed_data_size=200; compressed_data_size=200;"
                     "config={enable_parameterized=%true; desired_tablet_count=2};"
                     "tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=1; memory_size=0; compressed_data_size=100; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=200; memory_size=0; compressed_data_size=100; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=1; node_address=home}];"
            "nodes=[{node_address=home; memory_used=1}]}",
            /*reshardDescriptors*/ "[{tablets=[2;]; tablet_count=2; data_size=200}]"),
        std::tuple( // SPLIT (by metric)
            "{config={groups={default={parameterized={metric=\"double([/statistics/memory_size])\"; enable_reshard=%true}}}};"
            "tables=[{in_memory_mode=uncompressed; uncompressed_data_size=200; compressed_data_size=200;"
                     "config={enable_parameterized=%true; desired_tablet_count=2};"
                     "tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=10; memory_size=100; compressed_data_size=100; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=10; memory_size=1; compressed_data_size=100; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=1; node_address=home}];"
            "nodes=[{node_address=home; memory_used=1}]}",
            /*reshardDescriptors*/ "[{tablets=[1;]; tablet_count=2; data_size=10}]"),
        std::tuple( // SPLIT (by metric, swapped)
            "{config={groups={default={parameterized={metric=\"double([/statistics/memory_size])\"; enable_reshard=%true}}}};"
            "tables=[{in_memory_mode=uncompressed; uncompressed_data_size=20; compressed_data_size=20;"
                     "config={enable_parameterized=%true; desired_tablet_count=2};"
                     "tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=10; memory_size=1; compressed_data_size=10; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=10; memory_size=100; compressed_data_size=10; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=1; node_address=home}];"
            "nodes=[{node_address=home; memory_used=1}]}",
            /*reshardDescriptors*/ "[{tablets=[2;]; tablet_count=2; data_size=10}]"),
        std::tuple( // SPLIT an empty table (do nothing)
            "{config={groups={default={parameterized={metric=\"1\"; enable_reshard=%true}}}};"
            "tables=[{in_memory_mode=none; uncompressed_data_size=0; compressed_data_size=0;"
                     "config={enable_parameterized=%true; desired_tablet_count=2};"
                     "tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=0; memory_size=0; compressed_data_size=0; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=0; node_address=home}];"
            "nodes=[{node_address=home; memory_used=0}]}",
            /*reshardDescriptors*/ "[]"),
        std::tuple( // Just right
            "{config={groups={default={parameterized={metric=\"double([/statistics/memory_size])\"; enable_reshard=%true}}}};"
            "tables=[{in_memory_mode=uncompressed; uncompressed_data_size=40; compressed_data_size=40;"
                     "config={enable_parameterized=%true; desired_tablet_count=2};"
                     "tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=10; memory_size=10; compressed_data_size=10; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=10; memory_size=10; compressed_data_size=10; partition_count=1}};"
            "{tablet_index=3; cell_index=1;"
                "statistics={uncompressed_data_size=15; memory_size=15; compressed_data_size=20; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=102; node_address=home}];"
            "nodes=[{node_address=home; memory_used=102}]}",
            /*reshardDescriptors*/ "[]"),
        std::tuple( // MERGE (size is too small)
            "{config={groups={default={parameterized={metric=\"double([/statistics/memory_size])\"; enable_reshard=%true}}}};"
            "tables=[{in_memory_mode=uncompressed; uncompressed_data_size=40; compressed_data_size=40;"
                     "config={enable_parameterized=%true; desired_tablet_count=2};"
                     "tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=10; memory_size=1; compressed_data_size=10; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=10; memory_size=1; compressed_data_size=10; partition_count=1}};"
            "{tablet_index=3; cell_index=1;"
                "statistics={uncompressed_data_size=30; memory_size=1; compressed_data_size=20; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=102; node_address=home}];"
            "nodes=[{node_address=home; memory_used=102}]}",
            /*reshardDescriptors*/ "[{tablets=[1;2;]; tablet_count=1; data_size=20}]"),
        std::tuple( // MERGE (metric is too small)
            "{config={groups={default={parameterized={metric=\"double([/statistics/memory_size])\"; enable_reshard=%true}}}};"
            "tables=[{in_memory_mode=uncompressed; uncompressed_data_size=40; compressed_data_size=40;"
                     "config={enable_parameterized=%true; desired_tablet_count=2};"
                     "tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=10; memory_size=1; compressed_data_size=10; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=10; memory_size=1; compressed_data_size=10; partition_count=1}};"
            "{tablet_index=3; cell_index=1;"
                "statistics={uncompressed_data_size=15; memory_size=3; compressed_data_size=20; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=102; node_address=home}];"
            "nodes=[{node_address=home; memory_used=102}]}",
            /*reshardDescriptors*/ "[{tablets=[1;2;]; tablet_count=1; data_size=20}]"),
        std::tuple( // MERGE (both are less than desired)
            "{config={groups={default={parameterized={metric=\"double([/statistics/memory_size])\"; enable_reshard=%true}}}};"
            "tables=[{in_memory_mode=uncompressed; uncompressed_data_size=40; compressed_data_size=40;"
                     "config={enable_parameterized=%true; desired_tablet_count=2};"
                     "tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=10; memory_size=1; compressed_data_size=10; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=10; memory_size=1; compressed_data_size=10; partition_count=1}};"
            "{tablet_index=3; cell_index=1;"
                "statistics={uncompressed_data_size=20; memory_size=2; compressed_data_size=20; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=102; node_address=home}];"
            "nodes=[{node_address=home; memory_used=102}]}",
            /*reshardDescriptors*/ "[{tablets=[1;2;]; tablet_count=1; data_size=20}]"),
        std::tuple( // Just fine
            "{config={groups={default={parameterized={metric=\"double([/statistics/memory_size])\"; enable_reshard=%true}}}};"
            "tables=[{in_memory_mode=uncompressed; uncompressed_data_size=40; compressed_data_size=40;"
                     "config={enable_parameterized=%true; desired_tablet_count=4};"
                     "tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=11; memory_size=38; compressed_data_size=10; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=11; memory_size=11; compressed_data_size=10; partition_count=1}};"
            "{tablet_index=3; cell_index=1;"
                "statistics={uncompressed_data_size=11; memory_size=11; compressed_data_size=10; partition_count=1}};"
            "{tablet_index=4; cell_index=1;"
                "statistics={uncompressed_data_size=11; memory_size=11; compressed_data_size=10; partition_count=1}};"
            "{tablet_index=5; cell_index=1;"
                "statistics={uncompressed_data_size=38; memory_size=11; compressed_data_size=20; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=102; node_address=home}];"
            "nodes=[{node_address=home; memory_used=102}]}",
            /*reshardDescriptors*/ "[]"),
        std::tuple( // DISABLE BALANCING HARD
            "{config={groups={default={parameterized={metric=\"1\"; enable_parameterized_reshard=%true}}}};"
            "tables=[{in_memory_mode=uncompressed; uncompressed_data_size=200; compressed_data_size=200;"
                     "config={enable_parameterized=%false; desired_tablet_count=2};"
                     "tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=200; memory_size=200; compressed_data_size=200; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=200; node_address=home}];"
            "nodes=[{node_address=home; memory_used=200; memory_limit=200}]}",
            /*reshardDescriptors*/ "[]")));

////////////////////////////////////////////////////////////////////////////////

class TTestMergeSplitTabletsParameterizedErrors
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        /*bundle*/ TStringBuf,
        /*errorText*/ TString>>
{ };

TEST_P(TTestMergeSplitTabletsParameterizedErrors, BalancingError)
{
    const auto& params = GetParam();
    auto bundleHolder = ConvertTo<TBundleHolderPtr>(TYsonStringBuf(std::get<0>(params)));
    FillObjectIdsInBundleHolder(bundleHolder);
    auto bundle = bundleHolder->CreateBundle();

    const auto& table = bundle->Tables.begin()->second;
    auto group = table->TableConfig->Group.value_or(DefaultGroupName);

    auto resharder = CreateParameterizedResharder(
        bundle,
        /*performanceCountersKeys*/ {},
        /*performanceCountersTableSchema*/ nullptr,
        TParameterizedResharderConfig{}.MergeWith(GetOrCrash(bundle->Config->Groups, group)->Parameterized),
        group,
        Logger);

    EXPECT_THROW_WITH_SUBSTRING(
        resharder->BuildTableActionDescriptors(table),
        ToString(std::get<1>(params)));
}

INSTANTIATE_TEST_SUITE_P(
    TTestMergeSplitTabletsParameterizedErrors,
    TTestMergeSplitTabletsParameterizedErrors,
    ::testing::Values(
        std::tuple(
            "{config={groups={default={parameterized={metric=\"-double([/statistics/memory_size])\"; enable_reshard=%true}}}};"
            "tables=[{in_memory_mode=uncompressed; "
                     "config={enable_parameterized=%true; desired_tablet_count=2};"
                     "tablets=["
            "{tablet_index=1; cell_index=1;"
                "statistics={uncompressed_data_size=50; memory_size=50; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=2; cell_index=1;"
                "statistics={uncompressed_data_size=20; memory_size=20; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=3; cell_index=2;"
                "statistics={uncompressed_data_size=10; memory_size=10; compressed_data_size=0; partition_count=1}};"
            "{tablet_index=4; cell_index=2;"
                "statistics={uncompressed_data_size=40; memory_size=40; compressed_data_size=0; partition_count=1}}]}];"
            "cells=[{cell_index=1; memory_size=70; node_address=home};"
                   "{cell_index=2; memory_size=50; node_address=home}];"
            "nodes=[{node_address=home; memory_used=0; memory_limit=100}]}",
            /*errorText*/ "Tablet metric must be nonnegative")));

////////////////////////////////////////////////////////////////////////////////

class TTestReshardDescriptorSorting
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::vector<TReshardDescriptor>>
{ };

TEST_P(TTestReshardDescriptorSorting, Sort)
{
    std::mt19937 generator;
    const auto& expectedDesciptors = GetParam();
    auto desciptors = expectedDesciptors;

    std::shuffle(desciptors.begin(), desciptors.end(), generator);
    std::sort(desciptors.begin(), desciptors.end());

    EXPECT_EQ(std::ssize(desciptors), std::ssize(expectedDesciptors));
    for (int index = 0; index < std::ssize(desciptors); ++index) {
        EXPECT_EQ(desciptors[index].Priority, expectedDesciptors[index].Priority);
    }
}

TReshardDescriptor CreateMergeDescriptor(int tabletToMerge, double minDeviation = 0.0)
{
    return TReshardDescriptor{
        .Priority = std::tuple(/*isSplit*/ false, -tabletToMerge, minDeviation)};
}

TReshardDescriptor CreateSplitDescriptor(int newTabletCount, double maxDeviation = 0.0)
{
    return TReshardDescriptor{
        .Priority = std::tuple(/*isSplit*/ true, -newTabletCount, -maxDeviation)};
}

INSTANTIATE_TEST_SUITE_P(
    TTestReshardDescriptorSorting,
    TTestReshardDescriptorSorting,
    ::testing::Values(
        std::vector<TReshardDescriptor>{CreateMergeDescriptor(1), CreateSplitDescriptor(1)},
        std::vector<TReshardDescriptor>{CreateMergeDescriptor(5), CreateMergeDescriptor(2)},
        std::vector<TReshardDescriptor>{CreateSplitDescriptor(5), CreateSplitDescriptor(2)},
        std::vector<TReshardDescriptor>{CreateMergeDescriptor(2, 100.), CreateMergeDescriptor(2, 200.)},
        std::vector<TReshardDescriptor>{CreateSplitDescriptor(2, 500.), CreateSplitDescriptor(2, 400.)},
        std::vector<TReshardDescriptor>{
            CreateMergeDescriptor(3, 50.),
            CreateMergeDescriptor(3, 100.),
            CreateMergeDescriptor(2, 50.),
            CreateMergeDescriptor(2, 100.),
            CreateSplitDescriptor(5, 400.),
            CreateSplitDescriptor(5, 100.),
            CreateSplitDescriptor(2, 300.),
            CreateSplitDescriptor(2, 200.)}));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletBalancer

#include <yt/yt/server/cell_balancer/chaos_scheduler.h>
#include <yt/yt/server/cell_balancer/config.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NCellBalancer {
namespace {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

constexpr TCellTag TestClockTag(212u);

TChaosSchedulerInputState GenerateSimpleInputContext()
{
    TChaosSchedulerInputState input;
    input.Config = New<TBundleControllerConfig>();
    input.Config->EnableChaosBundleManagement = true;
    input.GlobalRegistry = New<TGlobalCellRegistry>();

    const auto& registry = input.GlobalRegistry;
    registry->CellTagRangeBegin = 51000;
    registry->CellTagLast = 51005;
    registry->CellTagRangeEnd = 52000;

    const auto& chaosConfig = input.Config->ChaosConfig;
    chaosConfig->TabletCellClusters = {"seneca-ayt", "seneca-ist"};
    chaosConfig->ClockClusterTag = TestClockTag;

    return input;
}

////////////////////////////////////////////////////////////////////////////////

TCellTagInfoPtr CreateCellInfo(const TString& area, const TString& cellBundle, TCellTag cellTag)
{
    auto cellInfo = New<TCellTagInfo>();

    cellInfo->Area = area;
    cellInfo->CellBundle = cellBundle;
    cellInfo->CellId = NObjectClient::MakeRandomId(EObjectType::ChaosCell, cellTag);

    return cellInfo;
}

////////////////////////////////////////////////////////////////////////////////

void CheckEmptyAlerts(const TChaosSchedulerMutations& mutations)
{
    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));

    for (const auto& alert : mutations.AlertsToFire) {
        EXPECT_EQ("", alert.Id);
        EXPECT_EQ("", alert.BundleName);
        EXPECT_EQ("", alert.Description);
    }
}

////////////////////////////////////////////////////////////////////////////////

TBundleInfoPtr GetTabletCellBundleInfo(const TString& bundleName)
{
    auto bundleInfo = New<TBundleInfo>();
    bundleInfo->Health = NTabletClient::ETabletCellHealth::Good;
    bundleInfo->Zone = "default-zone";
    bundleInfo->NodeTagFilter = "default-zone/" + bundleName;
    bundleInfo->EnableBundleController = true;

    auto config = New<TBundleConfig>();
    bundleInfo->TargetConfig = config;
    return bundleInfo;
}

////////////////////////////////////////////////////////////////////////////////

TChaosBundleInfoPtr CreateChaosCellBundleInfo(bool initAreas = true)
{
    auto bundleInfo = New<TChaosBundleInfo>();
    bundleInfo->Areas["default"] = New<TBundleArea>();

    if (initAreas) {
        bundleInfo->Areas["beta"] = New<TBundleArea>();
    }

    return bundleInfo;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TChaosCellBundleManagement, TestTabletCellCreation)
{
    auto input = GenerateSimpleInputContext();
    input.TabletCellBundles["bigc"] = GetTabletCellBundleInfo("bigc");

    // Nothing happens for bundle that does not ask for chaos initialization
    TChaosSchedulerMutations mutations;
    ScheduleChaosBundles(input, &mutations);
    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.ForeignSystemAccountsToCreate));
    EXPECT_EQ(0, std::ssize(mutations.ForeignTabletCellBundlesToCreate));
    EXPECT_EQ(0, std::ssize(mutations.ForeignBundleCellTagsToSet));

    input.TabletCellBundles["bigc"]->TargetConfig->InitChaosBundles = true;
    mutations = TChaosSchedulerMutations{};
    ScheduleChaosBundles(input, &mutations);
    // Incomplete input data
    EXPECT_EQ(2, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "cannot_find_tablet_cell_bundles");
    EXPECT_EQ(0, std::ssize(mutations.ForeignSystemAccountsToCreate));
    EXPECT_EQ(0, std::ssize(mutations.ForeignTabletCellBundlesToCreate));
    EXPECT_EQ(0, std::ssize(mutations.ForeignBundleCellTagsToSet));

    // Setting required data
    input.ForeignTabletCellBundles["seneca-ist"] = {};
    input.ForeignTabletCellBundles["seneca-ayt"] = {};
    mutations = TChaosSchedulerMutations{};
    ScheduleChaosBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(2, std::ssize(mutations.ForeignSystemAccountsToCreate));
    EXPECT_EQ(2, std::ssize(mutations.ForeignTabletCellBundlesToCreate));
    EXPECT_EQ(0, std::ssize(mutations.ForeignBundleCellTagsToSet));

    // Checking account names
    EXPECT_EQ(mutations.ForeignSystemAccountsToCreate["seneca-ist"], THashSet<TString>{"bigc_bundle_system_quotas"});
    EXPECT_EQ(mutations.ForeignSystemAccountsToCreate["seneca-ayt"], THashSet<TString>{"bigc_bundle_system_quotas"});

    EXPECT_TRUE(mutations.ForeignTabletCellBundlesToCreate["seneca-ist"].contains("bigc"));
    EXPECT_TRUE(mutations.ForeignTabletCellBundlesToCreate["seneca-ayt"].contains("bigc"));
}

TEST(TChaosCellBundleManagement, TestSetClusterClockCellTag)
{
    auto input = GenerateSimpleInputContext();
    input.TabletCellBundles["bigc"] = GetTabletCellBundleInfo("bigc");

    input.TabletCellBundles["bigc"]->TargetConfig->InitChaosBundles = true;
    input.ForeignTabletCellBundles["seneca-ist"]["bigc"] = GetTabletCellBundleInfo("bigc");
    input.ForeignTabletCellBundles["seneca-ayt"]["bigc"] = GetTabletCellBundleInfo("bigc");

    auto mutations = TChaosSchedulerMutations{};
    ScheduleChaosBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ForeignSystemAccountsToCreate));
    EXPECT_EQ(0, std::ssize(mutations.ForeignTabletCellBundlesToCreate));
    EXPECT_EQ(2, std::ssize(mutations.ForeignBundleCellTagsToSet));
    EXPECT_EQ(2, std::ssize(mutations.ForeignBundleCellTagsToSet));
    EXPECT_EQ(mutations.ForeignBundleCellTagsToSet["seneca-ayt"]["bigc"], TestClockTag);
    EXPECT_EQ(mutations.ForeignBundleCellTagsToSet["seneca-ist"]["bigc"], TestClockTag);

    input.ForeignTabletCellBundles["seneca-ayt"]["bigc"]->Options->ClockClusterTag = TestClockTag;
    input.ForeignTabletCellBundles["seneca-ist"]["bigc"]->Options->ClockClusterTag = TestClockTag;

    mutations = TChaosSchedulerMutations{};
    ScheduleChaosBundles(input, &mutations);
    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ForeignSystemAccountsToCreate));
    EXPECT_EQ(0, std::ssize(mutations.ForeignTabletCellBundlesToCreate));
    EXPECT_EQ(0, std::ssize(mutations.ForeignBundleCellTagsToSet));

    input.ForeignTabletCellBundles["seneca-ayt"]["bigc"]->Options->ClockClusterTag = {};
    input.ForeignTabletCellBundles["seneca-ayt"]["bigc"]->TabletCellIds.resize(10);

    mutations = TChaosSchedulerMutations{};
    ScheduleChaosBundles(input, &mutations);
    EXPECT_EQ(1, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "cannot_set_clock_cluster_tag");
    EXPECT_EQ(0, std::ssize(mutations.ForeignSystemAccountsToCreate));
    EXPECT_EQ(0, std::ssize(mutations.ForeignTabletCellBundlesToCreate));
    EXPECT_EQ(0, std::ssize(mutations.ForeignBundleCellTagsToSet));
}

TEST(TChaosCellBundleManagement, TestChaosCellBundlesCreation)
{
    auto input = GenerateSimpleInputContext();
    const auto& chaosConfig = input.Config->ChaosConfig;
    chaosConfig->TabletCellClusters.clear();
    chaosConfig->ChaosCellClusters = {"chaos-alpha", "chaos-beta"};
    chaosConfig->AlphaChaosCluster = "chaos-alpha";
    chaosConfig->BetaChaosCluster = "chaos-beta";

    input.TabletCellBundles["bigc"] = GetTabletCellBundleInfo("bigc");

    // Nothing happens for bundle that does not ask for chaos initialization
    TChaosSchedulerMutations mutations;
    ScheduleChaosBundles(input, &mutations);
    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.ForeignSystemAccountsToCreate));
    EXPECT_EQ(0, std::ssize(mutations.ForeignTabletCellBundlesToCreate));
    EXPECT_EQ(0, std::ssize(mutations.ForeignBundleCellTagsToSet));

    input.TabletCellBundles["bigc"]->TargetConfig->InitChaosBundles = true;
    mutations = TChaosSchedulerMutations{};
    ScheduleChaosBundles(input, &mutations);
    // Incomplete input data
    EXPECT_EQ(2, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "cannot_find_chaos_cell_bundles");
    EXPECT_EQ(0, std::ssize(mutations.ForeignChaosCellBundlesToCreate));

    // Setting required data
    input.ForeignChaosCellBundles["chaos-alpha"] = {};
    input.ForeignChaosCellBundles["chaos-beta"] = {};
    mutations = TChaosSchedulerMutations{};
    ScheduleChaosBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(2, std::ssize(mutations.ForeignChaosCellBundlesToCreate));

    // Checking account names
    EXPECT_TRUE(mutations.ForeignChaosCellBundlesToCreate["chaos-beta"].contains("bigc"));
    EXPECT_TRUE(mutations.ForeignChaosCellBundlesToCreate["chaos-alpha"].contains("bigc"));
}

TEST(TChaosCellBundleManagement, TestCellsRegistry)
{
    auto input = GenerateSimpleInputContext();
    input.TabletCellBundles["bigc"] = GetTabletCellBundleInfo("bigc");
    input.Config->ChaosConfig->TabletCellClusters.clear();

    const auto& registry = input.GlobalRegistry;
    registry->CellTagRangeBegin = 51000;
    registry->CellTagLast = 51005;
    registry->CellTagRangeEnd = 51006;

    // Nothing happens for bundle that does not ask for chaos initialization.
    TChaosSchedulerMutations mutations;
    ScheduleChaosBundles(input, &mutations);
    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.CellTagsToRegister));
    EXPECT_FALSE(mutations.ChangedChaosCellTagLast);

    input.TabletCellBundles["bigc"]->TargetConfig->InitChaosBundles = true;

    mutations = TChaosSchedulerMutations{};
    ScheduleChaosBundles(input, &mutations);
    EXPECT_EQ(1, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "chaos_cell_tag_range_is_exhausted");
    EXPECT_EQ(0, std::ssize(mutations.CellTagsToRegister));
    EXPECT_FALSE(mutations.ChangedChaosCellTagLast);

    // Move the end of the range.
    registry->CellTagRangeEnd = 51008;
    mutations = TChaosSchedulerMutations{};
    ScheduleChaosBundles(input, &mutations);
    CheckEmptyAlerts(mutations);
    EXPECT_EQ(2, std::ssize(mutations.CellTagsToRegister));
    EXPECT_EQ(mutations.ChangedChaosCellTagLast, 51007);

    registry->CellTagLast = mutations.ChangedChaosCellTagLast.value();
    for (const auto& [cellTag, cellInfo] : mutations.CellTagsToRegister) {
        EXPECT_EQ(EObjectType::ChaosCell, TypeFromId(cellInfo->CellId));
        EXPECT_FALSE(IsWellKnownId(cellInfo->CellId))
            << "Cell id is well known: " << ToString(cellInfo->CellId);
        registry->CellTags[cellTag] = cellInfo;
    }

    mutations = TChaosSchedulerMutations{};
    ScheduleChaosBundles(input, &mutations);
    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.CellTagsToRegister));

    registry->CellTagRangeEnd = 51050;
    input.TabletCellBundles["bigc"]->TargetConfig->AdditionalChaosCellCount = 2;

    mutations = TChaosSchedulerMutations{};
    ScheduleChaosBundles(input, &mutations);
    CheckEmptyAlerts(mutations);

    EXPECT_EQ(0, std::ssize(mutations.CellTagsToRegister));
    EXPECT_EQ(4, std::ssize(mutations.AdditionalCellTagsToRegister));
    EXPECT_EQ(mutations.ChangedChaosCellTagLast, 51011);

    registry->CellTagLast = mutations.ChangedChaosCellTagLast.value();
    for (const auto& [cellTag, cellInfo] : mutations.AdditionalCellTagsToRegister) {
        EXPECT_EQ(EObjectType::ChaosCell, TypeFromId(cellInfo->CellId));
        EXPECT_FALSE(IsWellKnownId(cellInfo->CellId))
            << "Cell id is well known: " << ToString(cellInfo->CellId);
        registry->AdditionalCellTags[cellTag] = cellInfo;
    }

    mutations = TChaosSchedulerMutations{};
    ScheduleChaosBundles(input, &mutations);
    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.CellTagsToRegister));
    EXPECT_EQ(0, std::ssize(mutations.AdditionalCellTagsToRegister));
    EXPECT_EQ(mutations.ChangedChaosCellTagLast, std::nullopt);
}

TEST(TChaosCellBundleManagement, TestInconsistentCellsRegistry)
{
    auto input = GenerateSimpleInputContext();
    input.TabletCellBundles["bigc"] = GetTabletCellBundleInfo("bigc");
    input.Config->ChaosConfig->TabletCellClusters.clear();

    const auto& registry = input.GlobalRegistry;
    registry->CellTagRangeBegin = 51000;
    registry->CellTagLast = 27;
    registry->CellTagRangeEnd = 51006;
    input.TabletCellBundles["bigc"]->TargetConfig->InitChaosBundles = true;

    TChaosSchedulerMutations mutations;
    ScheduleChaosBundles(input, &mutations);
    EXPECT_EQ(0, std::ssize(mutations.CellTagsToRegister));
    EXPECT_FALSE(mutations.ChangedChaosCellTagLast);

    EXPECT_GE(1, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "global_cell_registry_is_inconsistent");
}

TEST(TChaosCellBundleManagement, TestCreateAreas)
{
    auto input = GenerateSimpleInputContext();
    input.TabletCellBundles["bigc"] = GetTabletCellBundleInfo("bigc");
    input.TabletCellBundles["bigc"]->TargetConfig->InitChaosBundles = true;

    const auto& chaosConfig = input.Config->ChaosConfig;
    chaosConfig->ChaosCellClusters = {"chaos-alpha", "chaos-beta"};
    chaosConfig->AlphaChaosCluster = "chaos-alpha";
    chaosConfig->BetaChaosCluster = "chaos-beta";
    chaosConfig->TabletCellClusters.clear();

    input.ForeignChaosCellBundles["chaos-alpha"]["bigc"] = CreateChaosCellBundleInfo(/*initAreas*/ false);
    input.ForeignChaosCellBundles["chaos-beta"]["bigc"] = CreateChaosCellBundleInfo(/*initAreas*/ false);

    auto mutations = TChaosSchedulerMutations{};
    ScheduleChaosBundles(input, &mutations);
    CheckEmptyAlerts(mutations);

    EXPECT_EQ(2, std::ssize(mutations.ForeignChaosAreasToCreate));

    for (const TString& cluster : {"chaos-alpha", "chaos-beta"}) {
        EXPECT_TRUE(mutations.ForeignChaosAreasToCreate[cluster].contains("bigc"));
    }
}

TEST(TChaosCellBundleManagement, TestCreateChaosCells)
{
    auto input = GenerateSimpleInputContext();
    input.TabletCellBundles["bigc"] = GetTabletCellBundleInfo("bigc");
    input.TabletCellBundles["bigc"]->TargetConfig->InitChaosBundles = true;

    const auto& registry = input.GlobalRegistry;
    registry->CellTagRangeBegin = 51000;
    registry->CellTagLast = 51005;
    registry->CellTagRangeEnd = 51008;
    registry->CellTags = {
        {TCellTag(51006), CreateCellInfo("default", "bigc", TCellTag{51006})},
        {TCellTag(51007), CreateCellInfo("beta", "bigc", TCellTag{51007})},
    };

    const auto& chaosConfig = input.Config->ChaosConfig;
    chaosConfig->ChaosCellClusters = {"chaos-alpha", "chaos-beta"};
    chaosConfig->AlphaChaosCluster = "chaos-alpha";
    chaosConfig->BetaChaosCluster = "chaos-beta";
    chaosConfig->TabletCellClusters.clear();

    input.ForeignChaosCellBundles["chaos-alpha"]["bigc"] = CreateChaosCellBundleInfo();
    input.ForeignChaosCellBundles["chaos-beta"]["bigc"] = CreateChaosCellBundleInfo();

    auto mutations = TChaosSchedulerMutations{};
    ScheduleChaosBundles(input, &mutations);
    CheckEmptyAlerts(mutations);

    EXPECT_EQ(0, std::ssize(mutations.ForeignChaosCellBundlesToCreate));
    EXPECT_EQ(0, std::ssize(mutations.ForeignChaosAreasToCreate));
    EXPECT_EQ(0, std::ssize(mutations.CellTagsToRegister));
    EXPECT_FALSE(mutations.ChangedChaosCellTagLast);

    EXPECT_EQ(2, std::ssize(mutations.ForeignChaosCellsToCreate));

    for (const TString& cluster : {"chaos-alpha", "chaos-beta"}) {
        const auto& cellsToCreate = mutations.ForeignChaosCellsToCreate[cluster];
        EXPECT_EQ(2, std::ssize(cellsToCreate));

        for (const auto& [_, cellInfo] : registry->CellTags) {
            EXPECT_TRUE(cellsToCreate.contains(ToString(cellInfo->CellId)));

            input.ForeignChaosCellBundles[cluster]["bigc"]->ChaosCellIds.insert(cellInfo->CellId);
        }
    }

    mutations = TChaosSchedulerMutations{};
    ScheduleChaosBundles(input, &mutations);
    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ForeignChaosCellsToCreate));

    registry->CellTagRangeEnd = 51050;
    registry->AdditionalCellTags = {
        {TCellTag(51010), CreateCellInfo("default", "bigc", TCellTag{51010})},
        {TCellTag(51011), CreateCellInfo("beta", "bigc", TCellTag{51011})},
        {TCellTag(51012), CreateCellInfo("default", "bigc", TCellTag{51012})},
        {TCellTag(51013), CreateCellInfo("beta", "bigc", TCellTag{51013})},
    };

    mutations = TChaosSchedulerMutations{};
    ScheduleChaosBundles(input, &mutations);
    CheckEmptyAlerts(mutations);
    EXPECT_EQ(2, std::ssize(mutations.ForeignChaosCellsToCreate));

    for (const TString& cluster : {"chaos-alpha", "chaos-beta"}) {
        const auto& cellsToCreate = mutations.ForeignChaosCellsToCreate[cluster];
        EXPECT_EQ(4, std::ssize(cellsToCreate));

        for (const auto& [_, cellInfo] : registry->AdditionalCellTags) {
            EXPECT_TRUE(cellsToCreate.contains(ToString(cellInfo->CellId)));

            input.ForeignChaosCellBundles[cluster]["bigc"]->ChaosCellIds.insert(cellInfo->CellId);
        }
    }

    mutations = TChaosSchedulerMutations{};
    ScheduleChaosBundles(input, &mutations);
    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ForeignChaosCellsToCreate));
}

TEST(TChaosCellBundleManagement, TestSetMetadataCells)
{
    auto input = GenerateSimpleInputContext();
    input.TabletCellBundles["bigc"] = GetTabletCellBundleInfo("bigc");
    input.TabletCellBundles["bigc"]->TargetConfig->InitChaosBundles = true;

    const auto& registry = input.GlobalRegistry;
    registry->CellTagRangeBegin = 51000;
    registry->CellTagLast = 51005;
    registry->CellTagRangeEnd = 51008;
    registry->CellTags = {
        {TCellTag(51006), CreateCellInfo("default", "bigc", TCellTag{51006})},
        {TCellTag(51007), CreateCellInfo("beta", "bigc", TCellTag{51007})},
    };

    const auto& chaosConfig = input.Config->ChaosConfig;
    chaosConfig->ChaosCellClusters = {"chaos-alpha", "chaos-beta"};
    chaosConfig->AlphaChaosCluster = "chaos-alpha";
    chaosConfig->BetaChaosCluster = "chaos-beta";
    chaosConfig->TabletCellClusters.clear();

    input.ForeignChaosCellBundles["chaos-alpha"]["bigc"] = CreateChaosCellBundleInfo();
    input.ForeignChaosCellBundles["chaos-beta"]["bigc"] = CreateChaosCellBundleInfo();

    auto mutations = TChaosSchedulerMutations{};
    ScheduleChaosBundles(input, &mutations);
    CheckEmptyAlerts(mutations);

    EXPECT_EQ(0, std::ssize(mutations.ForeignChaosCellBundlesToCreate));
    EXPECT_EQ(0, std::ssize(mutations.ForeignChaosAreasToCreate));
    EXPECT_EQ(0, std::ssize(mutations.CellTagsToRegister));
    EXPECT_FALSE(mutations.ChangedChaosCellTagLast);

    EXPECT_EQ(2, std::ssize(mutations.ForeignMetadataCellIdsToSet));

    for (const TString& cluster : {"chaos-alpha", "chaos-beta"}) {
        const auto& metadataCellIds = mutations.ForeignMetadataCellIdsToSet[cluster]["bigc"];
        EXPECT_EQ(2, std::ssize(metadataCellIds));

        for (const auto& [_, cellInfo] : registry->CellTags) {
            EXPECT_TRUE(metadataCellIds.contains(cellInfo->CellId));

            input.ForeignChaosCellBundles[cluster]["bigc"]->MetadataCellIds.insert(cellInfo->CellId);
        }
    }

    mutations = TChaosSchedulerMutations{};
    ScheduleChaosBundles(input, &mutations);
    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ForeignMetadataCellIdsToSet));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCellBalancer

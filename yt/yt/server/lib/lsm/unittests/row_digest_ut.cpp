#include <yt/yt/server/lib/lsm/compaction_hints.h>
#include <yt/yt/server/lib/lsm/partition.h>
#include <yt/yt/server/lib/lsm/tablet.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/table_client/versioned_row_digest.h>

#include <yt/yt/client/table_client/versioned_row.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/quantile_digest/config.h>
#include <yt/yt/library/quantile_digest/quantile_digest.h>

#include <random>

namespace NYT::NLsm {
namespace {

using namespace NHydra;
using namespace NTabletNode;
using namespace NTableClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

using TDigestFiller = std::function<std::pair<TTimestamp, TTimestamp>(const IVersionedRowDigestBuilderPtr&)>;

constexpr auto Kind = EStoreCompactionHintKind::VersionedRowDigest;
constexpr auto StartDate = TInstant::Days(7);
// CompactionTimestampAccuracy + Instant Timestamp conversions max error.
constexpr auto Accuracy = TDuration::Seconds(4);
constexpr auto StoreKind = EStoreCompactionHintKind::VersionedRowDigest;

std::mt19937 RandomGenerator(42);

struct TRowDigestTestParams
{
    TDuration MinDataTtl = TDuration::Days(1);
    TDuration MaxDataTtl = TDuration::Days(2);
    int MinDataVersions;
    int MaxDataVersions = 1;
    double MaxObsoleteTimestampRatio;
    int MaxTimestampsPerValue = 8192;
    TDigestFiller DigestFiller;

    TStoreCompactionHint Hint{Kind};
};

DEFINE_ENUM(EDigestFillKind,
    ((Write)          (0))
    ((Delete)         (1))
    ((Aggregate)      (2))
);

TDigestFiller CreateDigestFiller(
    int keyCount,
    int versionCount,
    int versionCountStep,
    EDigestFillKind fillKind = EDigestFillKind::Write,
    TDuration versionTimeStep = TDuration::Hours(1),
    TDuration startDateShift = TDuration::Zero())
{
    return [=] (const IVersionedRowDigestBuilderPtr& digestBuilder) {
        std::vector<int> keys(keyCount);
        std::iota(keys.begin(), keys.end(), 0);
        std::shuffle(keys.begin(), keys.end(), RandomGenerator);

        auto minStoreTimestamp = MaxTimestamp;
        auto maxStoreTimestamp = MinTimestamp;

        for (int i = 0; i < keyCount; ++i) {
            TVersionedRowBuilder rowBuilder(New<TRowBuffer>());
            rowBuilder.AddKey(MakeUnversionedInt64Value(keys[i]));

            for (int version = 1; version <= versionCount - versionCountStep * i; ++version) {
                auto timestamp = InstantToTimestamp(StartDate + startDateShift + version * versionTimeStep).first;
                minStoreTimestamp = std::min(minStoreTimestamp, timestamp);
                maxStoreTimestamp = std::max(maxStoreTimestamp, timestamp);

                switch (fillKind) {
                    case EDigestFillKind::Aggregate:
                    case EDigestFillKind::Write:
                        rowBuilder.AddValue(MakeVersionedInt64Value(
                            keys[i] + version,
                            timestamp));
                        break;

                    case EDigestFillKind::Delete:
                        rowBuilder.AddDeleteTimestamp(timestamp);
                        break;
                }
            }
            digestBuilder->OnRow(rowBuilder.FinishRow());
        }

        return std::pair(minStoreTimestamp, maxStoreTimestamp);
    };
}

double FloorWithPrecision(double value, i64 precision)
{
    double x = std::pow(10, precision);
    return std::floor(value * x) / x;
}

TTableSchemaPtr CreateSchemaForFillKind(EDigestFillKind fillKind)
{
    return New<TTableSchema>(
        std::vector<TColumnSchema>{
            TColumnSchema("value", EValueType::Int64)
                .SetAggregate(fillKind == EDigestFillKind::Aggregate
                    ? std::optional<std::string>("sum")
                    : std::nullopt)
        });
}

TStoreCompactionHint CreateHint(EStoreCompactionReason reason, TInstant timestamp = TInstant::Zero())
{
    // Being used only for logging.
    static auto tablet = New<TTablet>();
    static TStore store;
    store.SetTablet(tablet.Get());

    TStoreCompactionHint hint(Kind);

    hint.SetNodeObjectRevision(TRevision(1));
    if (reason != EStoreCompactionReason::None) {
        auto recalculationFinalizer = hint.BuildRecalculationFinalizer(&store);
        recalculationFinalizer.TryApplyRecalculation(timestamp, reason);
    }

    return hint;
}

TTDigestConfigPtr GetDigestConfig()
{
    auto DigestConfig =  New<TTDigestConfig>();
    DigestConfig->Delta = 0;

    return DigestConfig;
}

std::unique_ptr<TStore> MakeStore(
    TTabletPtr tablet,
    TDigestFiller filler,
    EDigestFillKind fillKind = EDigestFillKind::Write)
{
    auto store = std::make_unique<TStore>();
    store->SetTablet(tablet.Get());

    auto digestBuilder = CreateVersionedRowDigestBuilder(
        GetDigestConfig(),
        CreateSchemaForFillKind(fillKind));
    auto [minStoreTimestamp, maxStoreTimestamp] = filler(digestBuilder);

    store->SetMinTimestamp(minStoreTimestamp);
    store->SetMaxTimestamp(maxStoreTimestamp);

    auto& hint = store->CompactionHints().Hints()[Kind];
    hint = TStoreCompactionHint(Kind);
    hint.SetNodeObjectRevision(TRevision(1));

    store->CompactionHints().Payloads()[StoreKind] = digestBuilder->FlushDigest();

    return store;
}

std::unique_ptr<TStore> MakeStoreWithoutFirstDigest(
    TTabletPtr tablet,
    TDigestFiller filler,
    EDigestFillKind fillKind = EDigestFillKind::Write)
{
    auto store = MakeStore(std::move(tablet), std::move(filler), fillKind);
    std::get<TVersionedRowDigestPtr>(store->CompactionHints().Payloads()[StoreKind])->FirstTimestampDigest.Reset();
    return store;
}

struct TRowDigestTest
    : ::testing::TestWithParam<TRowDigestTestParams>
{ };

INSTANTIATE_TEST_SUITE_P(
    Lsm,
    TRowDigestTest,
    testing::Values(
        // TtlCleanupExpected with MinDataVersions = 1
        TRowDigestTestParams{
            .MinDataVersions = 1,
            .MaxObsoleteTimestampRatio = FloorWithPrecision(2. / 3, 6),
            .DigestFiller = CreateDigestFiller(100, 3, 0),
            .Hint = CreateHint(
                EStoreCompactionReason::TtlCleanupExpected,
                StartDate + TDuration::Days(1) + TDuration::Hours(3)),
        },
        TRowDigestTestParams{
            .MinDataVersions = 1,
            .MaxObsoleteTimestampRatio = FloorWithPrecision((3725. - 1) / 5050, 6),
            .DigestFiller = CreateDigestFiller(100, 100, 1),
            .Hint = CreateHint(
                EStoreCompactionReason::TtlCleanupExpected,
                StartDate + TDuration::Days(1) + TDuration::Hours(51)),
        },
        // TtlCleanupExpected with MinDataVersions = 0
        TRowDigestTestParams{
            .MinDataVersions = 0,
            .MaxDataVersions = 0,
            .MaxObsoleteTimestampRatio = FloorWithPrecision((3775. - 1) / 5050, 6),
            .DigestFiller = CreateDigestFiller(100, 100, 1),
            .Hint = CreateHint(
                EStoreCompactionReason::TtlCleanupExpected,
                StartDate + TDuration::Days(1) + TDuration::Hours(50)),
        },
        TRowDigestTestParams{
            .MaxDataTtl = TDuration::Hours(25),
            .MinDataVersions = 0,
            .MaxDataVersions = 1,
            .MaxObsoleteTimestampRatio = FloorWithPrecision(28. / 30, 6),
            .DigestFiller = CreateDigestFiller(5, 10, 2),
            .Hint = CreateHint(
                EStoreCompactionReason::TtlCleanupExpected,
                StartDate + TDuration::Hours(25) + TDuration::Hours(8)),
        },
        // TooManyTimestamps
        TRowDigestTestParams{
            .MinDataVersions = 1,
            .MaxObsoleteTimestampRatio = 0.6,
            .MaxTimestampsPerValue = 8192,
            .DigestFiller = CreateDigestFiller(2, 16384, 16383),
            .Hint = CreateHint(
                EStoreCompactionReason::TooManyTimestamps,
                StartDate + TDuration::Days(1) + TDuration::Hours(8192)),
        },
        // No compaction reason
        TRowDigestTestParams{
            .MinDataVersions = 1,
            .MaxObsoleteTimestampRatio = 1,
            .MaxTimestampsPerValue = 256,
            .DigestFiller = CreateDigestFiller(100, 255, 1),
            .Hint = CreateHint(EStoreCompactionReason::None),
        }));

TEST_P(TRowDigestTest, RowDigestTest)
{
    auto params = TRowDigestTest::GetParam();

    auto mountConfig = New<TTableMountConfig>();
    mountConfig->MinDataTtl = params.MinDataTtl;
    mountConfig->MaxDataTtl = params.MaxDataTtl;
    mountConfig->MinDataVersions = params.MinDataVersions;
    mountConfig->MaxDataVersions = params.MaxDataVersions;
    mountConfig->CompactionHints->RowDigest->EnableNonAggregates = true;
    mountConfig->CompactionHints->RowDigest->MaxObsoleteTimestampRatio = params.MaxObsoleteTimestampRatio;
    mountConfig->CompactionHints->RowDigest->MaxTimestampsPerValue = params.MaxTimestampsPerValue;

    auto digestConfig = New<TTDigestConfig>();
    digestConfig->Delta = 0;

    auto digestBuilder = CreateVersionedRowDigestBuilder(
        digestConfig,
        CreateSchemaForFillKind(EDigestFillKind::Write));
    params.DigestFiller(digestBuilder);

    auto tablet = New<TTablet>();
    tablet->SetMountConfig(std::move(mountConfig));

    auto store = MakeStore(tablet, params.DigestFiller);

    auto& hint = store->CompactionHints().Hints()[Kind];

    ASSERT_TRUE(hint.RecalculateHint(store));
    ASSERT_EQ(hint.GetReason(), params.Hint.GetReason());

    if (hint.GetReason() != EStoreCompactionReason::None) {
        ASSERT_NEAR(
            hint.GetTimestamp().GetValue(),
            params.Hint.GetTimestamp().GetValue(),
            Accuracy.GetValue());
    }
}

TEST(TRowDigestStoreTest, MissingFirstTimestampDigestSkipsTtlCleanup)
{
    auto mountConfig = New<TTableMountConfig>();
    mountConfig->MinDataVersions = 1;
    mountConfig->MaxDataVersions = 1;
    mountConfig->MinDataTtl = TDuration::Days(1);
    mountConfig->MaxDataTtl = TDuration::Days(2);
    mountConfig->CompactionHints->RowDigest->EnableNonAggregates = true;
    mountConfig->CompactionHints->RowDigest->MaxObsoleteTimestampRatio = FloorWithPrecision(2. / 3, 6);

    auto tablet = New<TTablet>();
    tablet->SetMountConfig(std::move(mountConfig));

    auto store = MakeStoreWithoutFirstDigest(tablet, CreateDigestFiller(100, 3, 0));
    auto& hint = store->CompactionHints().Hints()[Kind];

    ASSERT_TRUE(hint.RecalculateHint(store));
    EXPECT_EQ(hint.GetReason(), EStoreCompactionReason::None);
    EXPECT_EQ(hint.GetTimestamp(), TInstant::Zero());
}

////////////////////////////////////////////////////////////////////////////////

constexpr auto AggregateKind = EPartitionCompactionHintKind::AggregateVersionedRowDigest;

struct TAggregateConfigParams
{
    int MinDataVersions;
    int MaxDataVersions;
    TDuration MinDataTtl;
    TDuration MaxDataTtl;

    // Expected TTL timestamp for the "100 rows × 3 versions" data pattern.
    TInstant ExpectedTtlTimestamp3Versions() const
    {
        if (MinDataVersions == 1) {
            return StartDate + MinDataTtl + TDuration::Hours(3);
        }

        return StartDate + MinDataTtl + TDuration::Hours(2);
    }
};

struct TAggregateRowDigestTestBase
{
    TTabletPtr Tablet;
    TTableMountConfigPtr MountConfig;

    void SetUpWithParams(const TAggregateConfigParams& params)
    {
        MountConfig = New<TTableMountConfig>();
        MountConfig->MinDataTtl = params.MinDataTtl;
        MountConfig->MaxDataTtl = params.MaxDataTtl;
        MountConfig->MinDataVersions = params.MinDataVersions;
        MountConfig->MaxDataVersions = params.MaxDataVersions;
        MountConfig->CompactionHints->RowDigest->MaxObsoleteTimestampRatio =
            FloorWithPrecision(2. / 3, 6);

        Tablet = New<TTablet>();
        Tablet->SetMountConfig(MountConfig);
    }

    std::pair<std::unique_ptr<TPartition>, TPartitionCompactionHint*> MakePartition(int index = 1)
    {
        auto partition = std::make_unique<TPartition>();
        partition->SetTablet(Tablet.Get());
        partition->SetIndex(index);

        auto& hint = partition->CompactionHints().Hints()[AggregateKind];
        hint = TPartitionCompactionHint(StoreKind, AggregateKind);
        hint.SetNodeObjectRevision(TRevision(1));

        return {std::move(partition), &hint};
    }
};

struct TAggregateRowDigestTest
    : ::testing::TestWithParam<TAggregateConfigParams>
    , TAggregateRowDigestTestBase
{
    void SetUp() override
    {
        SetUpWithParams(GetParam());
    }
};

using TAggregateDeleteTestParam = std::tuple<TAggregateConfigParams, EDigestFillKind>;

struct TAggregateDeleteRowDigestTest
    : ::testing::TestWithParam<TAggregateDeleteTestParam>
    , TAggregateRowDigestTestBase
{
    void SetUp() override
    {
        SetUpWithParams(std::get<0>(GetParam()));
    }

    TAggregateConfigParams GetConfig() const
    {
        return std::get<0>(GetParam());
    }

    EDigestFillKind GetFillKind() const
    {
        return std::get<1>(GetParam());
    }
};

auto AllTabletConfigs = testing::Values(
    TAggregateConfigParams{
        .MinDataVersions = 0,
        .MaxDataVersions = 0,
        .MinDataTtl = TDuration::Hours(12),
        .MaxDataTtl = TDuration::Hours(36),
    },
    TAggregateConfigParams{
        .MinDataVersions = 0,
        .MaxDataVersions = 1,
        .MinDataTtl = TDuration::Hours(6),
        .MaxDataTtl = TDuration::Hours(25),
    },
    TAggregateConfigParams{
        .MinDataVersions = 1,
        .MaxDataVersions = 1,
        .MinDataTtl = TDuration::Days(1),
        .MaxDataTtl = TDuration::Days(2),
    });

INSTANTIATE_TEST_SUITE_P(
    AggregateConfigs,
    TAggregateRowDigestTest,
    AllTabletConfigs);

INSTANTIATE_TEST_SUITE_P(
    AggregateConfigs,
    TAggregateDeleteRowDigestTest,
    testing::Combine(
        AllTabletConfigs,
        testing::Values(EDigestFillKind::Delete, EDigestFillKind::Aggregate)));

////////////////////////////////////////////////////////////////////////////////

TEST_P(TAggregateRowDigestTest, SingleStoreTtlCleanupExpected)
{
    auto [partition, hint] = MakePartition();

    partition->Stores().push_back(MakeStore(Tablet, CreateDigestFiller(100, 3, 0)));

    ASSERT_TRUE(hint->RecalculateHint(partition.get()));
    EXPECT_EQ(hint->GetReason(), EStoreCompactionReason::AggregateTtlCleanupExpected);
    EXPECT_EQ(ssize(hint->StoreIds()), 1);
    EXPECT_NEAR(
        hint->GetTimestamp().GetValue(),
        GetParam().ExpectedTtlTimestamp3Versions().GetValue(),
        Accuracy.GetValue());
}

TEST_P(TAggregateRowDigestTest, SingleStoreNoCompactionNeeded)
{
    MountConfig->CompactionHints->RowDigest->MaxObsoleteTimestampRatio = 1.0;

    auto [partition, hint] = MakePartition();

    partition->Stores().push_back(MakeStore(Tablet, CreateDigestFiller(100, 3, 0)));

    ASSERT_TRUE(hint->RecalculateHint(partition.get()));
    EXPECT_EQ(hint->GetReason(), EStoreCompactionReason::None);
    EXPECT_EQ(hint->GetTimestamp(), TInstant::Zero());
}

TEST_P(TAggregateRowDigestTest, EmptyPartitionNoHint)
{
    auto [partition, hint] = MakePartition();

    ASSERT_TRUE(hint->RecalculateHint(partition.get()));
    EXPECT_EQ(hint->GetReason(), EStoreCompactionReason::None);
    EXPECT_EQ(hint->GetTimestamp(), TInstant::Zero());
    EXPECT_EQ(ssize(hint->StoreIds()), 0);
}

TEST_P(TAggregateDeleteRowDigestTest, TooManyTimestampsSingleStore)
{
    // Row 0: 16384 versions, row 1: 1 version. MaxTimestampsPerValue = 8192 = 2^13.
    // EarliestNthTimestamp[13] = StartDate + 8192 h.
    // compactionTimestamp = StartDate + 8192 h + MinDataTtl.
    // majorTimestamp = TInstant::Max() (single store) → condition fires.
    MountConfig->CompactionHints->RowDigest->MaxTimestampsPerValue = 8192;
    MountConfig->CompactionHints->RowDigest->MaxObsoleteTimestampRatio = 0.6;

    auto [partition, hint] = MakePartition();

    partition->Stores().push_back(MakeStore(
        Tablet,
        CreateDigestFiller(2, 16384, 16383, GetFillKind()),
        GetFillKind()));

    ASSERT_TRUE(hint->RecalculateHint(partition.get()));
    EXPECT_EQ(hint->GetReason(), EStoreCompactionReason::AggregateDeleteTooManyTimestamps);
    EXPECT_EQ(ssize(hint->StoreIds()), 1);
    EXPECT_NEAR(
        hint->GetTimestamp().GetValue(),
        (StartDate + GetConfig().MinDataTtl + TDuration::Hours(8192)).GetValue(),
        Accuracy.GetValue());
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TAggregateRowDigestTest, TwoStoresOldThenNewPrefixOfOne)
{
    // Store 0: old 3-version data at StartDate.
    // Store 1: fresh data at StartDate + 100 days (pushed first to verify timestamp-sort).
    // For prefix=1, majorTimestamp = stores[1]->minTimestamp (far future).
    // CalculateTtlCleanupExpected on store 0 alone already fires → prefix=1 wins.
    auto [partition, hint] = MakePartition();

    // Intentionally push in reverse order to verify sorting by min timestamp.
    partition->Stores().push_back(
        MakeStore(
            Tablet,
            CreateDigestFiller(100, 3, 0, EDigestFillKind::Write, TDuration::Hours(1), TDuration::Days(100))));
    partition->Stores().push_back(MakeStore(Tablet, CreateDigestFiller(100, 3, 0)));

    ASSERT_TRUE(hint->RecalculateHint(partition.get()));
    EXPECT_EQ(hint->GetReason(), EStoreCompactionReason::AggregateTtlCleanupExpected);
    EXPECT_EQ(ssize(hint->StoreIds()), 1);
    EXPECT_NEAR(
        hint->GetTimestamp().GetValue(),
        GetParam().ExpectedTtlTimestamp3Versions().GetValue(),
        Accuracy.GetValue());
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TAggregateRowDigestTest, FiveStoresMaxStoreCountExceededNoHint)
{
    // MaxStoreCount = 3, partition has 5 stores → implementation returns early → no hint.
    MountConfig->CompactionHints->RowDigest->MaxStoreCount = 3;

    auto [partition, hint] = MakePartition();

    for (int i = 0; i < 5; ++i) {
        partition->Stores().push_back(MakeStore(Tablet, CreateDigestFiller(100, 3, 0)));
    }

    ASSERT_TRUE(hint->RecalculateHint(partition.get()));
    EXPECT_EQ(hint->GetReason(), EStoreCompactionReason::None);
    EXPECT_EQ(hint->GetTimestamp(), TInstant::Zero());
    EXPECT_EQ(ssize(hint->StoreIds()), 0);
}

// Store 0 is old (3-version), stores 1-4 are far in the future → prefix=1 fires.
TEST_P(TAggregateRowDigestTest, FiveStoresMaxStoreCountEqualFiveHintFires)
{
    auto [partition, hint] = MakePartition();

    partition->Stores().push_back(MakeStore(Tablet, CreateDigestFiller(100, 3, 0)));
    for (int i = 1; i < 5; ++i) {
        partition->Stores().push_back(MakeStore(
            Tablet,
            CreateDigestFiller(10, 1, 0, EDigestFillKind::Write, TDuration::Hours(1), TDuration::Days(100))));
    }

    ASSERT_TRUE(hint->RecalculateHint(partition.get()));
    EXPECT_EQ(hint->GetReason(), EStoreCompactionReason::AggregateTtlCleanupExpected);
    EXPECT_EQ(ssize(hint->StoreIds()), 1);
    EXPECT_NEAR(
        hint->GetTimestamp().GetValue(),
        GetParam().ExpectedTtlTimestamp3Versions().GetValue(),
        TDuration::Hours(2).GetValue());
}

TEST_P(TAggregateDeleteRowDigestTest, FiveStoresTooManyTimestampsFirstPrefix)
{
    // Store 0: 2 rows, 16384 and 1 versions → EarliestNthTimestamp[13] = StartDate+8192h.
    // compactionTimestamp = StartDate + 8192h + MinDataTtl.
    // Stores 1-4 at StartDate+500days → majorTimestamp for prefix=1 is StartDate+500days > compactionTimestamp.
    // Condition fires at prefix=1 with AggregateDeleteTooManyTimestamps.
    MountConfig->CompactionHints->RowDigest->MaxTimestampsPerValue = 8192;
    MountConfig->CompactionHints->RowDigest->MaxObsoleteTimestampRatio = 0.6;

    auto [partition, hint] = MakePartition();

    partition->Stores().push_back(
        MakeStore(
            Tablet,
            CreateDigestFiller(2, 16384, 16383, GetFillKind()),
            GetFillKind()));

    for (int i = 1; i < 5; ++i) {
        partition->Stores().push_back(
            MakeStore(
                Tablet,
                CreateDigestFiller(10, 1, 0, EDigestFillKind::Write, TDuration::Hours(1), TDuration::Days(500))));
    }

    ASSERT_TRUE(hint->RecalculateHint(partition.get()));
    EXPECT_EQ(hint->GetReason(), EStoreCompactionReason::AggregateDeleteTooManyTimestamps);
    EXPECT_EQ(ssize(hint->StoreIds()), 1);
    EXPECT_NEAR(
        hint->GetTimestamp().GetValue(),
        (StartDate + GetConfig().MinDataTtl + TDuration::Hours(8192)).GetValue(),
        Accuracy.GetValue());
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TAggregateDeleteRowDigestTest, TooManyTimestampsBlockedByMajorTimestamp)
{
    // Store 0: 2 rows, 16384 and 1 versions → EarliestNthTimestamp[13] = StartDate+8192h.
    // compactionTimestamp = StartDate + 8192h + MinDataTtl.
    // Store 1 at StartDate + 10 days → majorTimestamp for prefix=1 = StartDate + 10 days.
    // Since compactionTimestamp >= majorTimestamp, the condition
    // `compactionTimestamp < majorTimestamp` is false → TooManyTimestamps does NOT fire
    // for prefix=1.
    // For prefix=2 (last prefix), majorTimestamp = TInstant::Max() → condition fires.
    // But TTL cleanup may also fire. We set MaxObsoleteTimestampRatio=1.0 to disable TTL.
    // So only TooManyTimestamps fires at prefix=2 with both stores.
    MountConfig->CompactionHints->RowDigest->MaxTimestampsPerValue = 8192;
    MountConfig->CompactionHints->RowDigest->MaxObsoleteTimestampRatio = 1.0;

    auto [partition, hint] = MakePartition();

    partition->Stores().push_back(MakeStore(
        Tablet,
        CreateDigestFiller(2, 16384, 16383, GetFillKind()),
        GetFillKind()));
    partition->Stores().push_back(
        MakeStore(Tablet, CreateDigestFiller(10, 1, 0, EDigestFillKind::Write, TDuration::Hours(1), TDuration::Days(10))));

    ASSERT_TRUE(hint->RecalculateHint(partition.get()));
    EXPECT_EQ(hint->GetReason(), EStoreCompactionReason::AggregateDeleteTooManyTimestamps);
    // Prefix=1 is blocked by majorTimestamp, so prefix=2 (both stores) wins.
    EXPECT_EQ(ssize(hint->StoreIds()), 2);
    EXPECT_NEAR(
        hint->GetTimestamp().GetValue(),
        (StartDate + GetConfig().MinDataTtl + TDuration::Hours(8192)).GetValue(),
        Accuracy.GetValue());
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TAggregateRowDigestTest, MissingFirstTimestampDigestSkipsTtlCleanup)
{
    auto [partition, hint] = MakePartition();
    partition->Stores().push_back(MakeStoreWithoutFirstDigest(Tablet, CreateDigestFiller(100, 3, 0)));

    ASSERT_TRUE(hint->RecalculateHint(partition.get()));
    EXPECT_EQ(
        hint->GetReason(),
        GetParam().MinDataVersions == 0
            ? EStoreCompactionReason::AggregateTtlCleanupExpected
            : EStoreCompactionReason::None);
}

TEST_P(TAggregateRowDigestTest, MissingFirstTimestampDigestInOneStoreSkipsTtlCleanup)
{
    auto [partition, hint] = MakePartition();
    partition->Stores().push_back(MakeStoreWithoutFirstDigest(Tablet, CreateDigestFiller(100, 3, 0)));
    partition->Stores().push_back(MakeStore(
        Tablet,
        CreateDigestFiller(100, 3, 0, EDigestFillKind::Aggregate, TDuration::Hours(1), TDuration::Days(100))));

    ASSERT_TRUE(hint->RecalculateHint(partition.get()));
    EXPECT_EQ(
        hint->GetReason(),
        GetParam().MinDataVersions == 0
            ? EStoreCompactionReason::AggregateTtlCleanupExpected
            : EStoreCompactionReason::None);
}

TEST_P(TAggregateRowDigestTest, MissingFirstTimestampDigestStillAllowsTooManyTimestamps)
{
    MountConfig->CompactionHints->RowDigest->MaxTimestampsPerValue = 8192;
    MountConfig->CompactionHints->RowDigest->MaxObsoleteTimestampRatio = 1.0;

    auto [partition, hint] = MakePartition();
    partition->Stores().push_back(MakeStoreWithoutFirstDigest(
        Tablet,
        CreateDigestFiller(2, 16384, 16383, EDigestFillKind::Aggregate),
        EDigestFillKind::Aggregate));

    ASSERT_TRUE(hint->RecalculateHint(partition.get()));
    EXPECT_EQ(hint->GetReason(), EStoreCompactionReason::AggregateDeleteTooManyTimestamps);
    EXPECT_NEAR(
        hint->GetTimestamp().GetValue(),
        (StartDate + GetParam().MinDataTtl + TDuration::Hours(8192)).GetValue(),
        Accuracy.GetValue());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NLsm

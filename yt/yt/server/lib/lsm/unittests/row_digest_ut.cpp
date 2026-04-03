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

using TDigestFiller = std::function<void(const IVersionedRowDigestBuilderPtr&)>;

constexpr auto Kind = EStoreCompactionHintKind::VersionedRowDigest;
constexpr auto StartDate = TInstant::Days(7);
// CompactionTimestampAccuracy + Instant Timestamp conversions max error.
constexpr auto Accuracy = TDuration::Seconds(4);

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

TDigestFiller CreateDigestFiller(
    int keyCount,
    int versionCount,
    int versionCountStep,
    TDuration versionTimeStep = TDuration::Hours(1),
    TDuration startDateShift = TDuration::Zero())
{
    return [=] (const IVersionedRowDigestBuilderPtr& digestBuilder) {
        std::vector<int> keys(keyCount);
        std::iota(keys.begin(), keys.end(), 0);
        std::shuffle(keys.begin(), keys.end(), RandomGenerator);

        for (int i = 0; i < keyCount; ++i) {
            TVersionedRowBuilder rowBuilder(New<TRowBuffer>());
            rowBuilder.AddKey(MakeUnversionedInt64Value(keys[i]));

            for (int version = 1; version <= versionCount - versionCountStep * i; ++version) {
                rowBuilder.AddValue(MakeVersionedInt64Value(
                    keys[i] + version,
                    InstantToTimestamp(StartDate + startDateShift + version * versionTimeStep).first));
            }
            digestBuilder->OnRow(rowBuilder.FinishRow());
        }
    };
}

double FloorWithPrecision(double value, i64 precision)
{
    double x = std::pow(10, precision);
    return std::floor(value * x) / x;
}

TStoreCompactionHint CreateHint(EStoreCompactionReason reason, TInstant timestamp = TInstant::Zero())
{
    TStoreCompactionHint hint(Kind);

    hint.SetNodeObjectRevision(TRevision(1));
    if (reason != EStoreCompactionReason::None) {
        auto recalculationFinalizer = hint.BuildRecalculationFinalizer();
        recalculationFinalizer.TryApplyRecalculation(timestamp, reason);
    }

    return hint;
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
                StartDate + TDuration::Days(1) + TDuration::Hours(8193)),
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

    auto digestBuilder = CreateVersionedRowDigestBuilder(digestConfig);
    params.DigestFiller(digestBuilder);

    auto tablet = New<TTablet>();
    tablet->SetMountConfig(std::move(mountConfig));

    auto store = std::make_unique<TStore>();
    store->SetTablet(tablet.Get());

    auto& hint = store->CompactionHints().Hints()[Kind];
    hint = TStoreCompactionHint(Kind);
    hint.SetNodeObjectRevision(TRevision(1));
    store->CompactionHints().Payloads()[Kind] = digestBuilder->FlushDigest();

    ASSERT_TRUE(hint.RecalculateHint(store));
    ASSERT_EQ(hint.GetReason(), params.Hint.GetReason());

    if (hint.GetReason() != EStoreCompactionReason::None) {
        ASSERT_NEAR(
            hint.GetTimestamp().GetValue(),
            params.Hint.GetTimestamp().GetValue(),
            Accuracy.GetValue());
    }
}

////////////////////////////////////////////////////////////////////////////////

constexpr auto AggregateKind = EPartitionCompactionHintKind::AggregateVersionedRowDigest;
constexpr auto StoreKind = EStoreCompactionHintKind::VersionedRowDigest;

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

struct TAggregateRowDigestTest
    : ::testing::TestWithParam<TAggregateConfigParams>
{
    TIntrusivePtr<TTablet> Tablet;
    TTableMountConfigPtr MountConfig;
    TTDigestConfigPtr DigestConfig;

    void SetUp() override
    {
        auto params = GetParam();

        MountConfig = New<TTableMountConfig>();
        MountConfig->MinDataTtl = params.MinDataTtl;
        MountConfig->MaxDataTtl = params.MaxDataTtl;
        MountConfig->MinDataVersions = params.MinDataVersions;
        MountConfig->MaxDataVersions = params.MaxDataVersions;
        MountConfig->CompactionHints->RowDigest->MaxObsoleteTimestampRatio =
            FloorWithPrecision(2. / 3, 6);

        DigestConfig = New<TTDigestConfig>();
        DigestConfig->Delta = 0;

        Tablet = New<TTablet>();
        Tablet->SetMountConfig(MountConfig);
    }

    std::unique_ptr<TStore> MakeStore(TDigestFiller filler, TTimestamp minTimestamp)
    {
        auto store = std::make_unique<TStore>();
        store->SetTablet(Tablet.Get());
        store->SetMinTimestamp(minTimestamp);

        auto digestBuilder = CreateVersionedRowDigestBuilder(DigestConfig);
        filler(digestBuilder);
        store->CompactionHints().Payloads()[StoreKind] = digestBuilder->FlushDigest();

        return store;
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

INSTANTIATE_TEST_SUITE_P(
    AggregateConfigs,
    TAggregateRowDigestTest,
    testing::Values(
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
        }));

////////////////////////////////////////////////////////////////////////////////

TEST_P(TAggregateRowDigestTest, SingleStoreTtlCleanupExpected)
{
    auto [partition, hint] = MakePartition();

    auto storeTs = InstantToTimestamp(StartDate).first;
    partition->Stores().push_back(MakeStore(CreateDigestFiller(100, 3, 0), storeTs));

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

    auto storeTs = InstantToTimestamp(StartDate).first;
    partition->Stores().push_back(MakeStore(CreateDigestFiller(100, 3, 0), storeTs));

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

TEST_P(TAggregateRowDigestTest, TooManyTimestampsSingleStore)
{
    // Row 0: 16384 versions, row 1: 1 version. MaxTimestampsPerValue = 8192 = 2^13.
    // EarliestNthTimestamp[13] = StartDate + 8193 h.
    // compactionTimestamp = StartDate + 8193 h + MinDataTtl.
    // majorTimestamp = TInstant::Max() (single store) → condition fires.
    MountConfig->CompactionHints->RowDigest->MaxTimestampsPerValue = 8192;
    MountConfig->CompactionHints->RowDigest->MaxObsoleteTimestampRatio = 0.6;

    auto [partition, hint] = MakePartition();

    auto storeTs = InstantToTimestamp(StartDate).first;
    partition->Stores().push_back(MakeStore(CreateDigestFiller(2, 16384, 16383), storeTs));

    ASSERT_TRUE(hint->RecalculateHint(partition.get()));
    EXPECT_EQ(hint->GetReason(), EStoreCompactionReason::AggregateDeleteTooManyTimestamps);
    EXPECT_EQ(ssize(hint->StoreIds()), 1);
    EXPECT_NEAR(
        hint->GetTimestamp().GetValue(),
        (StartDate + GetParam().MinDataTtl + TDuration::Hours(8193)).GetValue(),
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

    auto ts0 = InstantToTimestamp(StartDate).first;
    auto ts1 = InstantToTimestamp(StartDate + TDuration::Days(100)).first;

    // Intentionally push in reverse order to verify sorting by min timestamp.
    partition->Stores().push_back(MakeStore(CreateDigestFiller(100, 3, 0, TDuration::Hours(1), TDuration::Days(100)), ts1));
    partition->Stores().push_back(MakeStore(CreateDigestFiller(100, 3, 0), ts0));

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

    auto ts = InstantToTimestamp(StartDate).first;
    for (int i = 0; i < 5; ++i) {
        partition->Stores().push_back(MakeStore(CreateDigestFiller(100, 3, 0), ts + i));
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

    auto ts0 = InstantToTimestamp(StartDate).first;
    auto tsBig = InstantToTimestamp(StartDate + TDuration::Days(100)).first;

    partition->Stores().push_back(MakeStore(CreateDigestFiller(100, 3, 0), ts0));
    for (int i = 1; i < 5; ++i) {
        partition->Stores().push_back(MakeStore(CreateDigestFiller(10, 1, 0, TDuration::Hours(1), TDuration::Days(100)), tsBig + i));
    }

    ASSERT_TRUE(hint->RecalculateHint(partition.get()));
    EXPECT_EQ(hint->GetReason(), EStoreCompactionReason::AggregateTtlCleanupExpected);
    EXPECT_EQ(ssize(hint->StoreIds()), 1);
    EXPECT_NEAR(
        hint->GetTimestamp().GetValue(),
        GetParam().ExpectedTtlTimestamp3Versions().GetValue(),
        TDuration::Hours(2).GetValue());
}

TEST_P(TAggregateRowDigestTest, FiveStoresTooManyTimestampsFirstPrefix)
{
    // Store 0: 2 rows, 16384 and 1 versions → EarliestNthTimestamp[13] = StartDate+8193h.
    // compactionTimestamp = StartDate + 8193h + MinDataTtl.
    // Stores 1-4 at StartDate+500days → majorTimestamp for prefix=1 is StartDate+500days > compactionTimestamp.
    // Condition fires at prefix=1 with AggregateDeleteTooManyTimestamps.
    MountConfig->CompactionHints->RowDigest->MaxTimestampsPerValue = 8192;
    MountConfig->CompactionHints->RowDigest->MaxObsoleteTimestampRatio = 0.6;

    auto [partition, hint] = MakePartition();

    auto ts0 = InstantToTimestamp(StartDate).first;
    auto tsBig = InstantToTimestamp(StartDate + TDuration::Days(500)).first;

    partition->Stores().push_back(MakeStore(CreateDigestFiller(2, 16384, 16383), ts0));
    for (int i = 1; i < 5; ++i) {
        partition->Stores().push_back(MakeStore(CreateDigestFiller(10, 1, 0, TDuration::Hours(1), TDuration::Days(500)), tsBig));
    }

    ASSERT_TRUE(hint->RecalculateHint(partition.get()));
    EXPECT_EQ(hint->GetReason(), EStoreCompactionReason::AggregateDeleteTooManyTimestamps);
    EXPECT_EQ(ssize(hint->StoreIds()), 1);
    EXPECT_NEAR(
        hint->GetTimestamp().GetValue(),
        (StartDate + GetParam().MinDataTtl + TDuration::Hours(8193)).GetValue(),
        Accuracy.GetValue());
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TAggregateRowDigestTest, TooManyTimestampsBlockedByMajorTimestamp)
{
    // Store 0: 2 rows, 16384 and 1 versions → EarliestNthTimestamp[13] = StartDate+8193h.
    // compactionTimestamp = StartDate + 8193h + MinDataTtl.
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

    auto ts0 = InstantToTimestamp(StartDate).first;
    auto ts1 = InstantToTimestamp(StartDate + TDuration::Days(10)).first;

    partition->Stores().push_back(MakeStore(CreateDigestFiller(2, 16384, 16383), ts0));
    partition->Stores().push_back(MakeStore(CreateDigestFiller(10, 1, 0, TDuration::Hours(1), TDuration::Days(10)), ts1));

    ASSERT_TRUE(hint->RecalculateHint(partition.get()));
    EXPECT_EQ(hint->GetReason(), EStoreCompactionReason::AggregateDeleteTooManyTimestamps);
    // Prefix=1 is blocked by majorTimestamp, so prefix=2 (both stores) wins.
    EXPECT_EQ(ssize(hint->StoreIds()), 2);
    EXPECT_NEAR(
        hint->GetTimestamp().GetValue(),
        (StartDate + GetParam().MinDataTtl + TDuration::Hours(8193)).GetValue(),
        Accuracy.GetValue());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NLsm

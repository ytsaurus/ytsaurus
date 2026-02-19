#include <yt/yt/server/lib/lsm/compaction_hints.h>
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

TDigestFiller CreateDigestFillter(
    int keyCount,
    int versionCount,
    int versionCountStep,
    TDuration versionTimeStep = TDuration::Hours(1))
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
                    InstantToTimestamp(StartDate + version * versionTimeStep).first));
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
    hint.MakeDecision(timestamp, reason);

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
            .DigestFiller = CreateDigestFillter(100, 3, 0),
            .Hint = CreateHint(
                EStoreCompactionReason::TtlCleanupExpected,
                StartDate + TDuration::Days(1) + TDuration::Hours(3)),
        },
        TRowDigestTestParams{
            .MinDataVersions = 1,
            .MaxObsoleteTimestampRatio = FloorWithPrecision((3725. - 1) / 5050, 6),
            .DigestFiller = CreateDigestFillter(100, 100, 1),
            .Hint = CreateHint(
                EStoreCompactionReason::TtlCleanupExpected,
                StartDate + TDuration::Days(1) + TDuration::Hours(51)),
        },
        // TtlCleanupExpected with MinDataVersions = 0
        TRowDigestTestParams{
            .MinDataVersions = 0,
            .MaxDataVersions = 0,
            .MaxObsoleteTimestampRatio = FloorWithPrecision((3775. - 1) / 5050, 6),
            .DigestFiller = CreateDigestFillter(100, 100, 1),
            .Hint = CreateHint(
                EStoreCompactionReason::TtlCleanupExpected,
                StartDate + TDuration::Days(1) + TDuration::Hours(50)),
        },
        TRowDigestTestParams{
            .MaxDataTtl = TDuration::Hours(25),
            .MinDataVersions = 0,
            .MaxDataVersions = 1,
            .MaxObsoleteTimestampRatio = FloorWithPrecision(28. / 30, 6),
            .DigestFiller = CreateDigestFillter(5, 10, 2),
            .Hint = CreateHint(
                EStoreCompactionReason::TtlCleanupExpected,
                StartDate + TDuration::Hours(25) + TDuration::Hours(8)),
        },
        // TooManyTimestamps
        TRowDigestTestParams{
            .MinDataVersions = 1,
            .MaxObsoleteTimestampRatio = 0.6,
            .MaxTimestampsPerValue = 8192,
            .DigestFiller = CreateDigestFillter(2, 16384, 16383),
            .Hint = CreateHint(
                EStoreCompactionReason::TooManyTimestamps,
                StartDate + TDuration::Days(1) + TDuration::Hours(8193)),
        },
        // No compaction reason
        TRowDigestTestParams{
            .MinDataVersions = 1,
            .MaxObsoleteTimestampRatio = 1,
            .MaxTimestampsPerValue = 256,
            .DigestFiller = CreateDigestFillter(100, 255, 1),
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
    mountConfig->RowDigestCompaction->MaxObsoleteTimestampRatio = params.MaxObsoleteTimestampRatio;
    mountConfig->RowDigestCompaction->MaxTimestampsPerValue = params.MaxTimestampsPerValue;

    auto digestConfig = New<TVersionedRowDigestConfig>();
    digestConfig->Enable = true;
    digestConfig->TDigest->Delta = 0;

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

} // namespace
} // namespace NYT::NLsm

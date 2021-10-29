#include "tablet_write_manager_ut_helpers.h"

#include <yt/yt/client/table_client/row_buffer.h>

namespace NYT::NTabletNode {
namespace {

using namespace testing;

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

// Tests below check interaction between:
// - Tablet write manager
// - Transaction manager
// NB: original transaction supervisor is not used, simple transaction supervisor is used instead.

class TTestTabletWriteManager
    : public TTabletWriteManagerTestBase
{
protected:
    TTabletOptions GetOptions() const override
    {
        return TTabletOptions{};
    }

    TUnversionedOwningRow BuildRow(i64 key, std::optional<i64> value = std::nullopt)
    {
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedInt64Value(key, /*id*/ 0));
        if (value) {
            builder.AddValue(MakeUnversionedInt64Value(*value, /*id*/ 1));
        }
        return builder.FinishRow();
    }

    TVersionedOwningRow BuildVersionedRow(i64 key, std::vector<std::pair<ui64, i64>> values)
    {
        TVersionedRowBuilder builder(RowBuffer_);
        builder.AddKey(MakeUnversionedInt64Value(key, /*id*/ 0));
        for (const auto& [timestamp, value] : values) {
            builder.AddValue(MakeVersionedInt64Value(value, timestamp, /*id*/ 1));
        }
        return TVersionedOwningRow(builder.FinishRow());
    }

    void RunRecoverRun(const std::function<void()>& callable)
    {
        callable();
        HydraManager()->SaveLoad();
        callable();
    }

private:
    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TTestTabletWriteManager, TestSimple)
{
    auto versionedTxId = MakeTabletTransactionId(EAtomicity::Full, TCellTag(42), TTimestamp(0x40), /*hash*/ 1);

    WaitFor(WriteVersionedRows(versionedTxId, {BuildVersionedRow(1, {{0x25, 1}})}))
        .ThrowOnError();

    EXPECT_EQ(1, HydraManager()->GetPendingMutationCount());
    HydraManager()->ApplyAll();

    PrepareTransactionCommit(versionedTxId, true, 0x50);
    CommitTransaction(versionedTxId, 0x60);

    EXPECT_EQ(2, HydraManager()->GetPendingMutationCount());
    HydraManager()->ApplyAll();

    auto result = VersionedLookupRow(BuildRow(1));
    EXPECT_EQ(
        ToString(BuildVersionedRow(1, {{0x25, 1}})),
        ToString(result));

    auto unversionedTxId = MakeTabletTransactionId(EAtomicity::Full, TCellTag(42), TTimestamp(0x70), /*hash*/ 0);

    WaitFor(WriteUnversionedRows(unversionedTxId, {BuildRow(1, 2)}))
        .ThrowOnError();

    EXPECT_EQ(1, HydraManager()->GetPendingMutationCount());
    HydraManager()->ApplyAll();

    PrepareTransactionCommit(unversionedTxId, true, 0x80);
    CommitTransaction(unversionedTxId, 0x90);

    EXPECT_EQ(2, HydraManager()->GetPendingMutationCount());
    HydraManager()->ApplyAll();

    result = VersionedLookupRow(BuildRow(1));
    EXPECT_EQ(
        ToString(BuildVersionedRow(1, {{0x25, 1}, {0x90, 2}})),
        ToString(result));
}

TEST_F(TTestTabletWriteManager, TestWriteBarrierUnversionedPrepared)
{
    auto unversionedTxId = MakeTabletTransactionId(EAtomicity::Full, TCellTag(42), TTimestamp(0x10), /*hash*/ 0);

    WaitFor(WriteUnversionedRows(unversionedTxId, {BuildRow(1, 1)}))
        .ThrowOnError();

    EXPECT_EQ(1, HydraManager()->GetPendingMutationCount());
    HydraManager()->ApplyAll();

    PrepareTransactionCommit(unversionedTxId, true, 0x20);

    EXPECT_EQ(1, HydraManager()->GetPendingMutationCount());
    HydraManager()->ApplyAll();

    auto versionedTxId = MakeTabletTransactionId(EAtomicity::Full, TCellTag(42), TTimestamp(0x40), /*hash*/ 1);

    RunRecoverRun([&] {
        EXPECT_THAT(
            [&] {
                WaitFor(WriteVersionedRows(versionedTxId, {BuildVersionedRow(1, {{0x25, 2}})}))
                    .ThrowOnError();
            },
            ThrowsMessage<std::exception>(HasSubstr("user writes are still pending")));
    });
}

TEST_F(TTestTabletWriteManager, TestWriteBarrierUnversionedActive)
{
    auto unversionedTxId = MakeTabletTransactionId(EAtomicity::Full, TCellTag(42), TTimestamp(0x10), /*hash*/ 0);

    WaitFor(WriteUnversionedRows(unversionedTxId, {BuildRow(1, 1)}))
        .ThrowOnError();

    EXPECT_EQ(1, HydraManager()->GetPendingMutationCount());
    HydraManager()->ApplyAll();

    auto versionedTxId = MakeTabletTransactionId(EAtomicity::Full, TCellTag(42), TTimestamp(0x40), /*hash*/ 1);

    RunRecoverRun([&] {
        EXPECT_THAT(
            [&] {
                WaitFor(WriteVersionedRows(versionedTxId, {BuildVersionedRow(1, {{0x25, 2}})}))
                    .ThrowOnError();
            },
            ThrowsMessage<std::exception>(HasSubstr("user writes are still pending")));
    });
}

TEST_F(TTestTabletWriteManager, TestWriteBarrierUnversionedInFlight)
{
    auto unversionedTxId = MakeTabletTransactionId(EAtomicity::Full, TCellTag(42), TTimestamp(0x10), /*hash*/ 0);

    WaitFor(WriteUnversionedRows(unversionedTxId, {BuildRow(1, 1)}))
        .ThrowOnError();

    EXPECT_EQ(1, HydraManager()->GetPendingMutationCount());

    auto versionedTxId = MakeTabletTransactionId(EAtomicity::Full, TCellTag(42), TTimestamp(0x40), /*hash*/ 1);

    EXPECT_THAT(
        [&] {
            WaitFor(WriteVersionedRows(versionedTxId, {BuildVersionedRow(1, {{0x25, 2}})}))
                .ThrowOnError();
        },
        ThrowsMessage<std::exception>(HasSubstr("user mutations are still in flight")));

    // NB: in contrast to previous two tests, we cannot expect the same error after recovering from snapshot.
    // Note that WriteRows is not accepted during recovery.
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletNode

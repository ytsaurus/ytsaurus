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
    auto versionedTxId = MakeTabletTransactionId(TTimestamp(0x40));

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

    auto unversionedTxId = MakeTabletTransactionId(TTimestamp(0x70));

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
    auto unversionedTxId = MakeTabletTransactionId(TTimestamp(0x10));

    WaitFor(WriteUnversionedRows(unversionedTxId, {BuildRow(1, 1)}))
        .ThrowOnError();

    EXPECT_EQ(1, HydraManager()->GetPendingMutationCount());
    HydraManager()->ApplyAll();

    PrepareTransactionCommit(unversionedTxId, true, 0x20);

    EXPECT_EQ(1, HydraManager()->GetPendingMutationCount());
    HydraManager()->ApplyAll();

    auto versionedTxId = MakeTabletTransactionId(TTimestamp(0x40));

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
    auto unversionedTxId = MakeTabletTransactionId(TTimestamp(0x10));

    WaitFor(WriteUnversionedRows(unversionedTxId, {BuildRow(1, 1)}))
        .ThrowOnError();

    EXPECT_EQ(1, HydraManager()->GetPendingMutationCount());
    HydraManager()->ApplyAll();

    auto versionedTxId = MakeTabletTransactionId(TTimestamp(0x40));

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
    auto unversionedTxId = MakeTabletTransactionId(TTimestamp(0x10));

    WaitFor(WriteUnversionedRows(unversionedTxId, {BuildRow(1, 1)}))
        .ThrowOnError();

    EXPECT_EQ(1, HydraManager()->GetPendingMutationCount());

    auto versionedTxId = MakeTabletTransactionId(TTimestamp(0x40));

    EXPECT_THAT(
        [&] {
            WaitFor(WriteVersionedRows(versionedTxId, {BuildVersionedRow(1, {{0x25, 2}})}))
                .ThrowOnError();
        },
        ThrowsMessage<std::exception>(HasSubstr("user mutations are still in flight")));

    // NB: in contrast to previous two tests, we cannot expect the same error after recovering from snapshot.
    // Note that WriteRows is not accepted during recovery.
}

TEST_F(TTestTabletWriteManager, TestSignaturesSuccess)
{
    auto txId = MakeTabletTransactionId(TTimestamp(0x10));

    WaitFor(WriteUnversionedRows(txId, {BuildRow(0, 42)}, /*signature*/ 1))
        .ThrowOnError();

    WaitFor(WriteUnversionedRows(txId, {BuildRow(1, 42)}, /*signatures*/ FinalTransactionSignature - 1))
        .ThrowOnError();

    EXPECT_EQ(2, HydraManager()->GetPendingMutationCount());
    HydraManager()->ApplyAll();

    auto asyncCommit = PrepareAndCommitTransaction(txId, true, 0x20);

    EXPECT_EQ(2, HydraManager()->GetPendingMutationCount());

    HydraManager()->ApplyAll();

    asyncCommit
        .Get()
        .ThrowOnError();

    EXPECT_EQ(
        ToString(BuildVersionedRow(0, {{0x20, 42}})),
        ToString(VersionedLookupRow(BuildRow(0))));
    EXPECT_EQ(
        ToString(BuildVersionedRow(1, {{0x20, 42}})),
        ToString(VersionedLookupRow(BuildRow(1))));
}

TEST_F(TTestTabletWriteManager, TestSignaturesFailure)
{
    auto txId = MakeTabletTransactionId(TTimestamp(0x10));

    WaitFor(WriteUnversionedRows(txId, {BuildRow(0, 42)}, /*signature*/ 1))
        .ThrowOnError();

    EXPECT_EQ(1, HydraManager()->GetPendingMutationCount());
    HydraManager()->ApplyAll();

    auto asyncCommit = PrepareAndCommitTransaction(txId, true, 0x20);

    EXPECT_EQ(2, HydraManager()->GetPendingMutationCount());

    HydraManager()->ApplyAll();

    EXPECT_THAT(
        [&] {
            asyncCommit
                .Get()
                .ThrowOnError();
        },
        ThrowsMessage<std::exception>(HasSubstr("expected signature")));
}


////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletNode

#include "sorted_dynamic_store_ut_helpers.h"
#include "sorted_store_helpers.h"

#include <yt/yt/server/node/tablet_node/sorted_store_manager.h>

#include <yt/yt/client/table_client/wire_protocol.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

class TSortedStoreManagerTestBase
    : public TStoreManagerTestBase<TSortedStoreTestBase>
{
protected:
    IStoreManagerPtr CreateStoreManager(TTablet* tablet) override
    {
        YT_VERIFY(!StoreManager_);
        StoreManager_ = New<TSortedStoreManager>(
            New<TTabletManagerConfig>(),
            tablet,
            &TabletContext_);
        return StoreManager_;
    }

    IStoreManagerPtr GetStoreManager() override
    {
        return StoreManager_;
    }

    TSortedDynamicRowRef WriteRow(
        TTestTransaction* transaction,
        TUnversionedRow row,
        bool prelock)
    {
        auto context = transaction->CreateWriteContext();
        context.Phase = prelock ? EWritePhase::Prelock : EWritePhase::Lock;

        return StoreManager_->ModifyRow(row, NApi::ERowModificationType::Write, TLockMask(), &context);
    }

    TSortedDynamicRowRef WriteAndLockRow(
        TTestTransaction* transaction,
        TUnversionedRow row,
        TLockMask lockMask,
        bool prelock)
    {
        auto context = transaction->CreateWriteContext();
        context.Phase = prelock ? EWritePhase::Prelock : EWritePhase::Lock;
        return StoreManager_->ModifyRow(row, NApi::ERowModificationType::Write, lockMask, &context);
    }

    void WriteRow(const TUnversionedOwningRow& row, bool prelock = false)
    {
        auto transaction = StartTransaction();

        auto context = transaction->CreateWriteContext();
        context.Phase = prelock ? EWritePhase::Prelock : EWritePhase::Lock;
        auto rowRef = StoreManager_->ModifyRow(row, NApi::ERowModificationType::Write, TLockMask(), &context);

        if (prelock) {
            EXPECT_EQ(1u, transaction->PrelockedRows().size());
            EXPECT_EQ(rowRef, transaction->PrelockedRows().front());
        } else {
            EXPECT_EQ(1u, transaction->LockedRows().size());
            EXPECT_EQ(rowRef, transaction->LockedRows().front());
        }

        PrepareTransaction(transaction.get());
        StoreManager_->PrepareRow(transaction.get(), rowRef);

        CommitTransaction(transaction.get());

        NTableClient::TWriteRowCommand command{
            .Row = row
        };
        StoreManager_->CommitRow(transaction.get(), command, rowRef);
    }

    TSortedDynamicRowRef DeleteRow(
        TTestTransaction* transaction,
        TUnversionedRow row,
        bool prelock)
    {
        auto context = transaction->CreateWriteContext();
        context.Phase = prelock ? EWritePhase::Prelock : EWritePhase::Lock;

        return StoreManager_->ModifyRow(row, ERowModificationType::Delete, TLockMask(), &context);
    }

    void DeleteRow(const TLegacyOwningKey& key)
    {
        auto transaction = StartTransaction();

        DeleteRow(transaction.get(), key, false);

        EXPECT_EQ(1u, transaction->LockedRows().size());
        auto rowRef = transaction->LockedRows()[0];

        PrepareTransaction(transaction.get());
        StoreManager_->PrepareRow(transaction.get(), rowRef);

        CommitTransaction(transaction.get());

        NTableClient::TDeleteRowCommand command{
            .Row = key
        };
        StoreManager_->CommitRow(transaction.get(), command, rowRef);
    }

    void PrepareRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef)
    {
        StoreManager_->PrepareRow(transaction, rowRef);
    }

    void CommitRow(
        TTransaction* transaction,
        const NTableClient::TWireProtocolWriteCommand& command,
        const TSortedDynamicRowRef& rowRef)
    {
        StoreManager_->CommitRow(transaction, command, rowRef);
    }

    void AbortRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef)
    {
        StoreManager_->AbortRow(transaction, rowRef);
    }

    void ConfirmRow(TTestTransaction* transaction, const TSortedDynamicRowRef& rowRef)
    {
        auto writeContext = transaction->CreateWriteContext();
        StoreManager_->ConfirmRow(&writeContext, rowRef);
    }

    using TSortedStoreTestBase::LookupRow;

    TUnversionedOwningRow LookupRow(
        const TLegacyOwningKey& key,
        TTimestamp timestamp,
        const std::vector<int>& columnIndexes = {},
        TTabletSnapshotPtr tabletSnapshot = nullptr)
    {
        return LookupRowsImpl(
            Tablet_.get(),
            {key},
            TReadTimestampRange{
                .Timestamp = timestamp,
            },
            columnIndexes,
            tabletSnapshot,
            ChunkReadOptions_)[0];
    }

    TVersionedOwningRow VersionedLookupRow(
        const TLegacyOwningKey& key,
        int minDataVersions = 100,
        TTimestamp timestamp = AsyncLastCommittedTimestamp)
    {
        return VersionedLookupRowImpl(
            Tablet_.get(),
            key,
            minDataVersions,
            timestamp,
            ChunkReadOptions_);
    }

    TSortedDynamicStorePtr GetActiveStore()
    {
        return Tablet_->GetActiveStore()->AsSortedDynamic();
    }

    using TStoreSnapshot = std::pair<TString, TCallback<void(TSaveContext&)>>;

    TStoreSnapshot BeginReserializeTablet()
    {
        TString buffer;

        TStringOutput output(buffer);
        auto checkpointableOutput = CreateBufferedCheckpointableOutputStream(&output);
        TSaveContext saveContext(checkpointableOutput.get());
        Tablet_->Save(saveContext);
        saveContext.Finish();

        return std::make_pair(buffer, Tablet_->AsyncSave());
    }

    void EndReserializeTablet(const TStoreSnapshot& snapshot)
    {
        auto buffer = snapshot.first;

        TStringOutput output(buffer);
        auto checkpointableOutput = CreateBufferedCheckpointableOutputStream(&output);
        TSaveContext saveContext(checkpointableOutput.get());
        snapshot.second(saveContext);
        saveContext.Finish();

        TStringInput input(buffer);
        auto checkpointableInput = CreateCheckpointableInputStream(&input);

        StoreManager_.Reset();
        CreateTablet(/*revive*/ true);
        {
            TLoadContext loadContext(checkpointableInput.get());
            loadContext.SetVersion(GetCurrentReign());
            Tablet_->Load(loadContext);
        }
        {
            TLoadContext loadContext(checkpointableInput.get());
            loadContext.SetVersion(GetCurrentReign());
            Tablet_->AsyncLoad(loadContext);
        }

        Tablet_->StartEpoch(nullptr);

        StoreManager_.Reset();
        CreateStoreManager(Tablet_.get());
    }

    void ReserializeTablet()
    {
        EndReserializeTablet(BeginReserializeTablet());
    }


    TSortedStoreManagerPtr StoreManager_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode


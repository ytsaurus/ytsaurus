#include "sorted_dynamic_store_ut_helpers.h"

#include <yt/yt/server/node/tablet_node/lookup.h>
#include <yt/yt/server/node/tablet_node/sorted_store_manager.h>

#include <yt/yt/client/table_client/wire_protocol.h>
#include <yt/yt_proto/yt/client/table_chunk_format/proto/wire_protocol.pb.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

inline TVersionedOwningRow VersionedLookupRowImpl(
    TTablet* tablet,
    const TLegacyOwningKey& key,
    int minDataVersions = 100,
    TTimestamp timestamp = AsyncLastCommittedTimestamp,
    TClientChunkReadOptions chunkReadOptions = TClientChunkReadOptions())
{
    TSharedRef request;
    {
        TReqVersionedLookupRows req;
        std::vector<TUnversionedRow> keys(1, key);

        auto writer = CreateWireProtocolWriter();
        writer->WriteMessage(req);
        writer->WriteSchemafulRowset(keys);

        struct TMergedTag { };
        request = MergeRefsToRef<TMergedTag>(writer->Finish());
    }

    TSharedRef response;
    {
        auto retentionConfig = New<NTableClient::TRetentionConfig>();
        retentionConfig->MinDataVersions = minDataVersions;
        retentionConfig->MaxDataVersions = minDataVersions;

        auto reader = CreateWireProtocolReader(request);
        auto writer = CreateWireProtocolWriter();
        VersionedLookupRows(
            tablet->BuildSnapshot(nullptr),
            timestamp,
            false,
            chunkReadOptions,
            retentionConfig,
            reader.get(),
            writer.get());
        struct TMergedTag { };
        response = MergeRefsToRef<TMergedTag>(writer->Finish());
    }

    {
        auto reader = CreateWireProtocolReader(response);
        auto schemaData = IWireProtocolReader::GetSchemaData(*tablet->GetPhysicalSchema(), TColumnFilter());
        auto row = reader->ReadVersionedRow(schemaData, false);
        return TVersionedOwningRow(row);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TSortedStoreManagerTestBase
    : public TStoreManagerTestBase<TSortedDynamicStoreTestBase>
{
public:
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
        TTransaction* transaction,
        TUnversionedRow row,
        bool prelock)
    {
        TWriteContext context;
        context.Phase = prelock ? EWritePhase::Prelock : EWritePhase::Lock;
        context.Transaction = transaction;

        return StoreManager_->ModifyRow(row, NApi::ERowModificationType::Write, TLockMask(), &context);
    }

    TSortedDynamicRowRef WriteAndLockRow(
        TTransaction* transaction,
        TUnversionedRow row,
        TLockMask lockMask,
        bool prelock)
    {
        TWriteContext context;
        context.Phase = prelock ? EWritePhase::Prelock : EWritePhase::Lock;
        context.Transaction = transaction;
        return StoreManager_->ModifyRow(row, NApi::ERowModificationType::Write, lockMask, &context);
    }

    void WriteRow(const TUnversionedOwningRow& row, bool prelock = false)
    {
        auto transaction = StartTransaction();

        TWriteContext context;
        context.Phase = prelock ? EWritePhase::Prelock : EWritePhase::Lock;
        context.Transaction = transaction.get();
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
        TTransaction* transaction,
        TUnversionedRow row,
        bool prelock)
    {
        TWriteContext context;
        context.Phase = prelock ? EWritePhase::Prelock : EWritePhase::Lock;
        context.Transaction = transaction;

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

    void ConfirmRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef)
    {
        StoreManager_->ConfirmRow(transaction, rowRef);
    }

    using TSortedDynamicStoreTestBase::LookupRow;

    TUnversionedOwningRow LookupRow(const TLegacyOwningKey& key, TTimestamp timestamp)
    {
        return LookupRow(key, timestamp, /*columnIndexes*/ {}, Tablet_->BuildSnapshot(nullptr));
    }

    TUnversionedOwningRow LookupRow(
        const TLegacyOwningKey& key,
        TTimestamp timestamp,
        const std::vector<int>& columnIndexes,
        const TTabletSnapshotPtr& tabletSnapshot)
    {
        TSharedRef request;
        {
            TReqLookupRows req;
            if (!columnIndexes.empty()) {
                ToProto(req.mutable_column_filter()->mutable_indexes(), columnIndexes);
            }
            std::vector<TUnversionedRow> keys(1, key);

            auto writer = CreateWireProtocolWriter();
            writer->WriteMessage(req);
            writer->WriteSchemafulRowset(keys);

            struct TMergedTag { };
            request = MergeRefsToRef<TMergedTag>(writer->Finish());
        }

        TSharedRef response;
        {
            auto reader = CreateWireProtocolReader(request);
            auto writer = CreateWireProtocolWriter();
            LookupRows(
                tabletSnapshot,
                TReadTimestampRange{
                    .Timestamp = timestamp,
                },
                false,
                ChunkReadOptions_,
                reader.get(),
                writer.get());
            struct TMergedTag { };
            response = MergeRefsToRef<TMergedTag>(writer->Finish());
        }

        {
            auto reader = CreateWireProtocolReader(response);
            auto schemaData = IWireProtocolReader::GetSchemaData(*Tablet_->GetPhysicalSchema(), TColumnFilter());
            auto row = reader->ReadSchemafulRow(schemaData, false);
            return TUnversionedOwningRow(row);
        }
    }

    TVersionedOwningRow VersionedLookupRow(const TLegacyOwningKey& key, int minDataVersions = 100, TTimestamp timestamp = AsyncLastCommittedTimestamp)
    {
        return VersionedLookupRowImpl(Tablet_.get(), key, minDataVersions, timestamp, ChunkReadOptions_);
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
        TSaveContext saveContext;
        saveContext.SetVersion(static_cast<int>(GetCurrentReign()));
        saveContext.SetOutput(&output);
        Tablet_->Save(saveContext);

        return std::make_pair(buffer, Tablet_->AsyncSave());
    }

    void EndReserializeTablet(const TStoreSnapshot& snapshot)
    {
        auto buffer = snapshot.first;

        TStringOutput output(buffer);
        TSaveContext saveContext;
        saveContext.SetVersion(static_cast<int>(GetCurrentReign()));
        saveContext.SetOutput(&output);
        snapshot.second.Run(saveContext);

        TStringInput input(buffer);
        StoreManager_.Reset();
        CreateTablet(/*revive*/ true);
        {
            TLoadContext loadContext;
            loadContext.SetVersion(static_cast<int>(GetCurrentReign()));
            loadContext.SetInput(&input);
            Tablet_->Load(loadContext);
        }
        {
            TLoadContext loadContext;
            loadContext.SetVersion(static_cast<int>(GetCurrentReign()));
            loadContext.SetInput(&input);
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


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

        TWireProtocolWriter writer;
        writer.WriteMessage(req);
        writer.WriteSchemafulRowset(keys);

        struct TMergedTag { };
        request = MergeRefsToRef<TMergedTag>(writer.Finish());
    }

    TSharedRef response;
    {
        auto retentionConfig = New<NTableClient::TRetentionConfig>();
        retentionConfig->MinDataVersions = minDataVersions;
        retentionConfig->MaxDataVersions = minDataVersions;

        TWireProtocolReader reader(request);
        TWireProtocolWriter writer;
        VersionedLookupRows(
            tablet->BuildSnapshot(nullptr),
            timestamp,
            false,
            chunkReadOptions,
            retentionConfig,
            &reader,
            &writer);
        struct TMergedTag { };
        response = MergeRefsToRef<TMergedTag>(writer.Finish());
    }

    {
        TWireProtocolReader reader(response);
        auto schemaData = TWireProtocolReader::GetSchemaData(*tablet->GetPhysicalSchema(), TColumnFilter());
        auto row = reader.ReadVersionedRow(schemaData, false);
        return TVersionedOwningRow(row);
    }
}

class TSortedStoreManagerTestBase
    : public TStoreManagerTestBase<TSortedDynamicStoreTestBase>
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
        TTransaction* transaction,
        TUnversionedRow row,
        bool prelock)
    {
        TWriteContext context;
        context.Phase = prelock ? EWritePhase::Prelock : EWritePhase::Lock;
        context.Transaction = transaction;
        return StoreManager_->ModifyRow(row, NApi::ERowModificationType::Write, TLockMask(), &context);
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
        StoreManager_->CommitRow(transaction.get(), rowRef);
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
        StoreManager_->CommitRow(transaction.get(), rowRef);
    }

    void PrepareRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef)
    {
        StoreManager_->PrepareRow(transaction, rowRef);
    }

    void CommitRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef)
    {
        StoreManager_->CommitRow(transaction, rowRef);
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
        TSharedRef request;
        {
            TReqLookupRows req;
            std::vector<TUnversionedRow> keys(1, key);

            TWireProtocolWriter writer;
            writer.WriteMessage(req);
            writer.WriteSchemafulRowset(keys);

            struct TMergedTag { };
            request = MergeRefsToRef<TMergedTag>(writer.Finish());
        }

        TSharedRef response;
        {
            TWireProtocolReader reader(request);
            TWireProtocolWriter writer;
            LookupRows(
                Tablet_->BuildSnapshot(nullptr),
                TReadTimestampRange{
                    .Timestamp = timestamp,
                },
                false,
                ChunkReadOptions_,
                &reader,
                &writer);
            struct TMergedTag { };
            response = MergeRefsToRef<TMergedTag>(writer.Finish());
        }

        {
            TWireProtocolReader reader(response);
            auto schemaData = TWireProtocolReader::GetSchemaData(*Tablet_->GetPhysicalSchema(), TColumnFilter());
            auto row = reader.ReadSchemafulRow(schemaData, false);
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

    TSortedStoreManagerPtr StoreManager_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode


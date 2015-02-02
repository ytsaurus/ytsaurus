#include "stdafx.h"
#include "lookup.h"
#include "tablet.h"
#include "store.h"
#include "row_merger.h"
#include "tablet_slot.h"
#include "private.h"

#include <core/misc/object_pool.h>
#include <core/misc/protobuf_helpers.h>

#include <core/concurrency/scheduler.h>

#include <core/logging/log.h>

#include <ytlib/tablet_client/wire_protocol.h>
#include <ytlib/tablet_client/wire_protocol.pb.h>

#include <ytlib/new_table_client/versioned_lookuper.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NVersionedTableClient;
using namespace NTabletClient;
using namespace NTabletClient::NProto;

////////////////////////////////////////////////////////////////////////////////

const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

struct TLookupPoolTag { };

class TLookupSession
{
public:
    TLookupSession()
        : MemoryPool_(TLookupPoolTag())
        , RunCallback_(BIND(&TLookupSession::DoRun, this))
    { }

    void Prepare(
        TTabletSnapshotPtr tabletSnapshot,
        TTimestamp timestamp,
        TWireProtocolReader* reader)
    {
        Clean();

        TabletSnapshot_ = std::move(tabletSnapshot);
        Timestamp_ = timestamp;
        KeyColumnCount_ = TabletSnapshot_->KeyColumns.size();
        SchemaColumnCount_ = TabletSnapshot_->Schema.Columns().size();

        TReqLookupRows req;
        reader->ReadMessage(&req);

        if (req.has_column_filter()) {
            ColumnFilter_.All = false;
            ColumnFilter_.Indexes = FromProto<int, SmallVector<int, TypicalColumnCount>>(req.column_filter().indexes());
        } else {
            ColumnFilter_.All = true;
        }

        ValidateColumnFilter(ColumnFilter_, SchemaColumnCount_);

        reader->ReadUnversionedRowset(&LookupKeys_);
    }

    TFutureHolder<void> Run(
        IInvokerPtr invoker,
        TWireProtocolWriter* writer)
    {
        if (invoker) {
            return RunCallback_.AsyncVia(invoker).Run(writer);
        } else {
            try {
                RunCallback_.Run(writer);
                return VoidFuture;
            } catch (const std::exception& ex) {
                return MakeFuture(TError(ex));
            }
        }
    }

    const std::vector<TUnversionedRow>& GetLookupKeys() const
    {
        return LookupKeys_;
    }


    void Clean()
    {
        MemoryPool_.Clear();
        LookupKeys_.clear();
        EdenLookupers_.clear();
        PartitionLookupers_.clear();
    }

private:
    TChunkedMemoryPool MemoryPool_;
    std::vector<TUnversionedRow> LookupKeys_;
    std::vector<IVersionedLookuperPtr> EdenLookupers_;
    std::vector<IVersionedLookuperPtr> PartitionLookupers_;

    TTabletSnapshotPtr TabletSnapshot_;
    TTimestamp Timestamp_;
    int KeyColumnCount_;
    int SchemaColumnCount_;
    TColumnFilter ColumnFilter_;

    TCallback<void(TWireProtocolWriter* writer)> RunCallback_;


    void CreateLookupers(
        std::vector<IVersionedLookuperPtr>* lookupers,
        const TPartitionSnapshotPtr partitionSnapshot)
    {
        lookupers->clear();
        if (partitionSnapshot) {
            for (const auto& store : partitionSnapshot->Stores) {
                auto lookuper = store->CreateLookuper(Timestamp_, ColumnFilter_);
                lookupers->push_back(std::move(lookuper));
            }
        }
    }

    void InvokeLookupers(
        const std::vector<IVersionedLookuperPtr>& lookupers,
        TUnversionedRowMerger* merger,
        std::vector<TFutureHolder<TVersionedRow>>* rowHolders,
        TKey key)
    {
        for (const auto& lookuper : lookupers) {
            auto rowHolder = lookuper->Lookup(key);
            auto maybeRowOrError = rowHolder.Get().TryGet();
            if (maybeRowOrError) {
                merger->AddPartialRow(maybeRowOrError->ValueOrThrow());
            } else {
                rowHolders->push_back(std::move(rowHolder));
            }
        }
    }

    void DoRun(TWireProtocolWriter* writer)
    {
        CreateLookupers(&EdenLookupers_, TabletSnapshot_->Eden);

        TUnversionedRowMerger merger(
            &MemoryPool_,
            SchemaColumnCount_,
            KeyColumnCount_,
            ColumnFilter_);

        // Assuming that lookup keys are sorted, we cache the lookupers for the last
        // examined partition.
        TPartitionSnapshotPtr currentPartitionSnapshot;

        for (auto key : LookupKeys_) {
            ValidateServerKey(key, KeyColumnCount_, TabletSnapshot_->Schema);

            auto partitionSnapshot = TabletSnapshot_->FindContainingPartition(key);
            if (partitionSnapshot != currentPartitionSnapshot) {
                currentPartitionSnapshot = std::move(partitionSnapshot);
                CreateLookupers(&PartitionLookupers_, currentPartitionSnapshot);
            }

            // Send requests, collect sync responses.
            std::vector<TFutureHolder<TVersionedRow>> rowHolders;
            InvokeLookupers(EdenLookupers_, &merger, &rowHolders, key);
            InvokeLookupers(PartitionLookupers_, &merger, &rowHolders, key);

            // Wait for async responses.
            if (!rowHolders.empty()) {
                auto rows = WaitFor(Combine(rowHolders))
                    .ValueOrThrow();
                for (auto row : rows) {
                    merger.AddPartialRow(row);
                }
            }

            // Merge partial rows.
            auto mergedRow = merger.BuildMergedRow();
            writer->WriteUnversionedRow(mergedRow);
        }
    }

};

} // namespace NTabletNode
} // namespace NYT

namespace NYT {

template <>
struct TPooledObjectTraits<NTabletNode::TLookupSession, void>
    : public TPooledObjectTraitsBase
{
    static void Clean(NTabletNode::TLookupSession* executor)
    {
        executor->Clean();
    }
};

} // namespace NYT

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

void LookupRows(
    IInvokerPtr poolInvoker,
    TTabletSnapshotPtr tabletSnapshot,
    TTimestamp timestamp,
    TWireProtocolReader* reader,
    TWireProtocolWriter* writer)
{
    auto session = ObjectPool<TLookupSession>().Allocate();
    session->Prepare(tabletSnapshot, timestamp, reader);
    LOG_DEBUG("Looking up %v keys (TabletId: %v, CellId: %v, Session: %v)",
        session->GetLookupKeys().size(),
        tabletSnapshot->TabletId,
        tabletSnapshot->Slot ? tabletSnapshot->Slot->GetCellId() : NullCellId,
        session.get());

    auto resultHolder = session->Run(std::move(poolInvoker), writer);
    return WaitFor(resultHolder.Get())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

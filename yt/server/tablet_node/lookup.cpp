#include "stdafx.h"
#include "lookup.h"
#include "tablet.h"
#include "store.h"
#include "row_merger.h"
#include "tablet_slot.h"
#include "private.h"

#include <core/misc/object_pool.h>
#include <core/misc/protobuf_helpers.h>

#include <core/concurrency/parallel_collector.h>
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

class TLookupExecutor
{
public:
    TLookupExecutor()
        : MemoryPool_(TLookupPoolTag())
        , RunCallback_(BIND(&TLookupExecutor::DoRun, this).Guarded())
    { }

    void Prepare(
        TTabletSnapshotPtr tabletSnapshot,
        TTimestamp timestamp,
        TWireProtocolReader* reader)
    {
        TabletSnapshot_ = std::move(tabletSnapshot);
        KeyColumnCount_ = TabletSnapshot_->KeyColumns.size();
        SchemaColumnCount_ = TabletSnapshot_->Schema.Columns().size();

        TReqLookupRows req;
        reader->ReadMessage(&req);

        if (req.has_column_filter()) {
            ColumnFilter_.All = false;
            ColumnFilter_.Indexes = FromProto<int, SmallVector<int, TypicalColumnCount>>(req.column_filter().indexes());
        }

        ValidateColumnFilter(ColumnFilter_, SchemaColumnCount_);

        auto addLookupers = [&] (const TPartitionSnapshotPtr& partitionSnapshot) {
            for (const auto& store : partitionSnapshot->Stores) {
                auto lookuper = store->CreateLookuper(timestamp, ColumnFilter_);
                Lookupers_.push_back(std::move(lookuper));
            }
        };
        addLookupers(TabletSnapshot_->Eden);
        for (const auto& partitionSnapshot : TabletSnapshot_->Partitions) {
            addLookupers(partitionSnapshot);
        }

        reader->ReadUnversionedRowset(&LookupKeys_);
    }

    TAsyncError Run(
        IInvokerPtr invoker,
        TWireProtocolWriter* writer)
    {
        if (invoker) {
            return RunCallback_.AsyncVia(invoker).Run(writer);
        } else {
            return MakeFuture(RunCallback_.Run(writer));
        }
    }

    const std::vector<TUnversionedRow>& GetLookupKeys() const
    {
        return LookupKeys_;
    }


    void Clean()
    {
        ColumnFilter_ = TColumnFilter();
        MemoryPool_.Clear();
        LookupKeys_.clear();
        Lookupers_.clear();
    }

private:
    TChunkedMemoryPool MemoryPool_;
    std::vector<TUnversionedRow> LookupKeys_;
    std::vector<IVersionedLookuperPtr> Lookupers_;

    TTabletSnapshotPtr TabletSnapshot_;
    int KeyColumnCount_;
    int SchemaColumnCount_;
    TColumnFilter ColumnFilter_;

    TCallback<TError(TWireProtocolWriter* writer)> RunCallback_;


    void DoRun(TWireProtocolWriter* writer)
    {
        TUnversionedRowMerger rowMerger(
            &MemoryPool_,
            SchemaColumnCount_,
            KeyColumnCount_,
            ColumnFilter_);

        TKeyComparer keyComparer(KeyColumnCount_);

        for (auto key : LookupKeys_) {
            ValidateServerKey(key, KeyColumnCount_, TabletSnapshot_->Schema);

            // Create lookupers, send requests, collect sync responses.
            TIntrusivePtr<TParallelCollector<TVersionedRow>> collector;
            SmallVector<TVersionedRow, TypicalStoreCount> partialRows;
            for (const auto& lookuper : Lookupers_) {
                auto futureRowOrError = lookuper->Lookup(key);
                auto maybeRowOrError = futureRowOrError.TryGet();
                if (maybeRowOrError) {
                    THROW_ERROR_EXCEPTION_IF_FAILED(*maybeRowOrError);
                    partialRows.push_back(maybeRowOrError->Value());
                } else {
                    if (!collector) {
                        collector = New<TParallelCollector<TVersionedRow>>();
                    }
                    collector->Collect(futureRowOrError);
                }
            }

            // Wait for async responses.
            if (collector) {
                auto result = WaitFor(collector->Complete());
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
                partialRows.insert(partialRows.end(), result.Value().begin(), result.Value().end());
            }

            // Merge partial rows.
            for (auto row : partialRows) {
                if (row) {
                    rowMerger.AddPartialRow(row);
                }
            }

            auto mergedRow = rowMerger.BuildMergedRowAndReset();
            writer->WriteUnversionedRow(mergedRow);
        }
    }

};

} // namespace NTabletNode
} // namespace NYT

namespace NYT {

template <>
struct TPooledObjectTraits<NTabletNode::TLookupExecutor, void>
    : public TPooledObjectTraitsBase
{
    static void Clean(NTabletNode::TLookupExecutor* executor)
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
    auto executor = ObjectPool<TLookupExecutor>().Allocate();
    executor->Prepare(std::move(tabletSnapshot), timestamp, reader);
    LOG_DEBUG("Looking up %v keys (TabletId: %v, CellId: %v)",
        executor->GetLookupKeys().size(),
        tabletSnapshot->TabletId,
        tabletSnapshot->Slot->GetCellId());

    auto result = WaitFor(executor->Run(poolInvoker, writer));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

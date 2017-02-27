#include "lookup.h"
#include "private.h"
#include "store.h"
#include "tablet.h"
#include "tablet_slot.h"

#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/row_merger.h>
#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/versioned_reader.h>

#include <yt/ytlib/tablet_client/wire_protocol.h>
#include <yt/ytlib/tablet_client/wire_protocol.pb.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/nullable.h>
#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/small_vector.h>
#include <yt/core/misc/variant.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;
static const size_t RowBufferCapacity = 1000;

////////////////////////////////////////////////////////////////////////////////

struct TLookupSessionBufferTag
{ };

class TLookupSession
{
public:
    TLookupSession(
        TTabletSnapshotPtr tabletSnapshot,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter,
        const TWorkloadDescriptor& workloadDescriptor,
        TSharedRange<TUnversionedRow> lookupKeys)
        : TabletSnapshot_(std::move(tabletSnapshot))
        , Timestamp_(timestamp)
        , ColumnFilter_(columnFilter)
        , WorkloadDescriptor_(workloadDescriptor)
        , LookupKeys_(std::move(lookupKeys))
    { }

    void Run(
        const std::function<void(TVersionedRow)>& onPartialRow,
        const std::function<void()>& onRow)
    {
        LOG_DEBUG("Tablet lookup started (TabletId: %v, CellId: %v, KeyCount: %v)",
            TabletSnapshot_->TabletId,
            TabletSnapshot_->CellId,
            LookupKeys_.Size());

        CreateReadSessions(&EdenSessions_, TabletSnapshot_->GetEdenStores(), LookupKeys_);

        auto currentIt = LookupKeys_.Begin();
        while (currentIt != LookupKeys_.End()) {
            auto nextPartitionIt = std::upper_bound(
                TabletSnapshot_->PartitionList.begin(),
                TabletSnapshot_->PartitionList.end(),
                *currentIt,
                [] (TKey lhs, const TPartitionSnapshotPtr& rhs) {
                    return lhs < rhs->PivotKey;
                });
            YCHECK(nextPartitionIt != TabletSnapshot_->PartitionList.begin());
            auto nextIt = nextPartitionIt == TabletSnapshot_->PartitionList.end()
                ? LookupKeys_.End()
                : std::lower_bound(currentIt, LookupKeys_.End(), (*nextPartitionIt)->PivotKey);

            LookupInPartition(
                *(nextPartitionIt - 1),
                LookupKeys_.Slice(currentIt, nextIt),
                onPartialRow,
                onRow);

            currentIt = nextIt;
        }

        LOG_DEBUG("Tablet lookup completed (TabletId: %v, CellId: %v, FoundRowCount: %v)",
            TabletSnapshot_->TabletId,
            TabletSnapshot_->CellId,
            FoundRowCount_);
    }

private:
    class TReadSession
    {
    public:
        explicit TReadSession(IVersionedReaderPtr reader)
            : Reader_(std::move(reader))
        {
            Rows_.reserve(RowBufferCapacity);
        }

        TVersionedRow FetchRow()
        {
            ++RowIndex_;
            if (RowIndex_ >= Rows_.size()) {
                RowIndex_ = 0;
                while (true) {
                    YCHECK(Reader_->Read(&Rows_));
                    if (!Rows_.empty()) {
                        break;
                    }
                    WaitFor(Reader_->GetReadyEvent())
                        .ThrowOnError();
                }
            }
            return Rows_[RowIndex_];
        }

    private:
        const IVersionedReaderPtr Reader_;

        std::vector<TVersionedRow> Rows_;

        int RowIndex_ = -1;

    };

    const TTabletSnapshotPtr TabletSnapshot_;
    const TTimestamp Timestamp_;
    const TColumnFilter& ColumnFilter_;
    const TWorkloadDescriptor& WorkloadDescriptor_;
    const TSharedRange<TUnversionedRow> LookupKeys_;

    static const int TypicalSessionCount = 16;
    using TReadSessionList = SmallVector<TReadSession, TypicalSessionCount>;
    TReadSessionList EdenSessions_;
    TReadSessionList PartitionSessions_;

    int FoundRowCount_ = 0;

    void CreateReadSessions(
        TReadSessionList* sessions,
        const std::vector<ISortedStorePtr>& stores,
        const TSharedRange<TKey>& keys)
    {
        sessions->clear();

        // NB: Will remain empty for in-memory tables.
        std::vector<TFuture<void>> asyncFutures;
        for (const auto& store : stores) {
            auto reader = store->CreateReader(
                TabletSnapshot_,
                keys,
                Timestamp_,
                ColumnFilter_,
                WorkloadDescriptor_);
            auto future = reader->Open();
            auto maybeError = future.TryGet();
            if (maybeError) {
                maybeError->ThrowOnError();
            } else {
                asyncFutures.emplace_back(std::move(future));
            }
            sessions->emplace_back(std::move(reader));
        }

        if (!asyncFutures.empty()) {
            WaitFor(Combine(asyncFutures))
                .ThrowOnError();
        }
    }

    void LookupInPartition(
        const TPartitionSnapshotPtr& partitionSnapshot,
        const TSharedRange<TKey>& keys,
        const std::function<void(TVersionedRow)>& onPartialRow,
        const std::function<void()>& onRow)
    {
        if (keys.Empty() || !partitionSnapshot) {
            return;
        }

        CreateReadSessions(&PartitionSessions_, partitionSnapshot->Stores, keys);

        auto processSessions = [&] (TReadSessionList& sessions) {
            for (auto& session : sessions) {
                onPartialRow(session.FetchRow());
            }
        };

        for (int index = 0; index < keys.Size(); ++index) {
            processSessions(PartitionSessions_);
            processSessions(EdenSessions_);

            onRow();

            ++FoundRowCount_;
        }
    }
};

static NTableClient::TColumnFilter DecodeColumnFilter(
    std::unique_ptr<NTabletClient::NProto::TColumnFilter> protoColumnFilter,
    int columnCount)
{
    NTableClient::TColumnFilter columnFilter;
    if (!protoColumnFilter) {
        columnFilter.All = true;
    } else {
        columnFilter.All = false;
        columnFilter.Indexes = FromProto<SmallVector<int, TypicalColumnCount>>(protoColumnFilter->indexes());
    }
    ValidateColumnFilter(columnFilter, columnCount);
    return columnFilter;
}

void LookupRows(
    TTabletSnapshotPtr tabletSnapshot,
    TTimestamp timestamp,
    const TWorkloadDescriptor& workloadDescriptor,
    TWireProtocolReader* reader,
    TWireProtocolWriter* writer)
{
    TReqLookupRows req;
    reader->ReadMessage(&req);

    auto columnFilter = DecodeColumnFilter(
        std::unique_ptr<NTabletClient::NProto::TColumnFilter>(req.release_column_filter()),
        tabletSnapshot->PhysicalSchema.GetColumnCount());
    auto schemaData = TWireProtocolReader::GetSchemaData(tabletSnapshot->PhysicalSchema.ToKeys());
    auto lookupKeys = reader->ReadSchemafulRowset(schemaData, false);

    TLookupSession session(
        tabletSnapshot,
        timestamp,
        columnFilter,
        workloadDescriptor,
        std::move(lookupKeys));

    TSchemafulRowMerger merger(
        New<TRowBuffer>(TLookupSessionBufferTag()),
        tabletSnapshot->PhysicalSchema.Columns().size(),
        tabletSnapshot->PhysicalSchema.GetKeyColumnCount(),
        columnFilter,
        tabletSnapshot->ColumnEvaluator);

    session.Run(
        [&] (TVersionedRow partialRow) { merger.AddPartialRow(partialRow); },
        [&] { writer->WriteSchemafulRow(merger.BuildMergedRow()); });
}

void VersionedLookupRows(
    TTabletSnapshotPtr tabletSnapshot,
    TTimestamp timestamp,
    const TWorkloadDescriptor& workloadDescriptor,
    TWireProtocolReader* reader,
    TWireProtocolWriter* writer)
{
    TReqVersionedLookupRows req;
    reader->ReadMessage(&req);

    auto columnFilter = DecodeColumnFilter(
        std::unique_ptr<NTabletClient::NProto::TColumnFilter>(req.release_column_filter()),
        tabletSnapshot->PhysicalSchema.GetColumnCount());
    auto schemaData = TWireProtocolReader::GetSchemaData(tabletSnapshot->PhysicalSchema.ToKeys());
    auto lookupKeys = reader->ReadSchemafulRowset(schemaData, false);

    TLookupSession session(
        tabletSnapshot,
        timestamp,
        columnFilter,
        workloadDescriptor,
        std::move(lookupKeys));

    TVersionedRowMerger merger(
        New<TRowBuffer>(TLookupSessionBufferTag()),
        tabletSnapshot->PhysicalSchema.GetColumnCount(),
        tabletSnapshot->PhysicalSchema.GetKeyColumnCount(),
        columnFilter,
        tabletSnapshot->Config,
        timestamp,
        MinTimestamp,
        tabletSnapshot->ColumnEvaluator);

    session.Run(
        [&] (TVersionedRow partialRow) { merger.AddPartialRow(partialRow); },
        [&] { writer->WriteVersionedRow(merger.BuildMergedRow()); });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

#include "stdafx.h"
#include "lookup.h"
#include "tablet.h"
#include "store.h"
#include "tablet_slot.h"
#include "private.h"

#include <core/misc/protobuf_helpers.h>
#include <core/misc/small_vector.h>

#include <core/concurrency/scheduler.h>

#include <core/logging/log.h>

#include <ytlib/tablet_client/wire_protocol.h>
#include <ytlib/tablet_client/wire_protocol.pb.h>

#include <ytlib/table_client/row_merger.h>

#include <ytlib/table_client/versioned_reader.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TLookupSession
{
public:
    TLookupSession(
        TTabletSnapshotPtr tabletSnapshot,
        TTimestamp timestamp,
        TWireProtocolReader* reader,
        TWireProtocolWriter* writer)
        : TabletSnapshot_(std::move(tabletSnapshot))
        , Timestamp_(timestamp)
        , Reader_(reader)
        , Writer_(writer)
        , KeyColumnCount_ (TabletSnapshot_->KeyColumns.size())
        , SchemaColumnCount_(TabletSnapshot_->Schema.Columns().size())
        , MemoryPool_(TLookupPoolTag())
    { }

    void Run()
    {
        TReqLookupRows req;
        Reader_->ReadMessage(&req);

        if (req.has_column_filter()) {
            ColumnFilter_.All = false;
            ColumnFilter_.Indexes = FromProto<int, SmallVector<int, TypicalColumnCount>>(req.column_filter().indexes());
        } else {
            ColumnFilter_.All = true;
        }

        ValidateColumnFilter(ColumnFilter_, SchemaColumnCount_);

        LookupKeys_ = Reader_->ReadUnversionedRowset();

        LOG_DEBUG("Performing tablet lookup (TabletId: %v, CellId: %v, KeyCount: %v)",
            TabletSnapshot_->TabletId,
            TabletSnapshot_->Slot ? TabletSnapshot_->Slot->GetCellId() : NullCellId,
            LookupKeys_.Size());

        CreateReadSessions(&EdenSessions_, TabletSnapshot_->Eden, LookupKeys_);

        TPartitionSnapshotPtr currentPartitionSnapshot;
        int currentPartitionStartOffset = 0;
        for (int index = 0; index < LookupKeys_.Size(); ++index) {
            YASSERT(index == 0 || LookupKeys_[index] >= LookupKeys_[index - 1]);
            auto key = LookupKeys_[index];
            ValidateServerKey(key, KeyColumnCount_, TabletSnapshot_->Schema);
            auto partitionSnapshot = TabletSnapshot_->FindContainingPartition(key);
            if (partitionSnapshot != currentPartitionSnapshot) {
                LookupInPartition(
                    currentPartitionSnapshot,
                    LookupKeys_.Slice(currentPartitionStartOffset, index));
                currentPartitionSnapshot = std::move(partitionSnapshot);
                currentPartitionStartOffset = index;
            }
        }

        LookupInPartition(
            currentPartitionSnapshot,
            LookupKeys_.Slice(currentPartitionStartOffset, LookupKeys_.Size()));
    }

private:
    class TReadSession
    {
    public:
        explicit TReadSession(IVersionedReaderPtr reader)
            : Reader_(std::move(reader))
        {
            Rows_.reserve(BufferCapacity);
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

        static const int BufferCapacity = 1000;
        std::vector<TVersionedRow> Rows_;

        int RowIndex_ = -1;

    };

    const TTabletSnapshotPtr TabletSnapshot_;
    const TTimestamp Timestamp_;
    TWireProtocolReader* const Reader_;
    TWireProtocolWriter* const Writer_;

    const int KeyColumnCount_;
    const int SchemaColumnCount_;

    struct TLookupPoolTag { };
    TChunkedMemoryPool MemoryPool_;

    TSharedRange<TUnversionedRow> LookupKeys_;

    static const int TypicalSessionCount = 16;
    using TReadSessionList = SmallVector<TReadSession, TypicalSessionCount>;
    TReadSessionList EdenSessions_;
    TReadSessionList PartitionSessions_;

    TColumnFilter ColumnFilter_;


    void CreateReadSessions(
        TReadSessionList* sessions,
        const TPartitionSnapshotPtr& partitionSnapshot,
        const TSharedRange<TKey>& keys)
    {
        sessions->clear();
        if (!partitionSnapshot) {
            return;
        }

        // NB: Will remain empty for in-memory tables.
        std::vector<TFuture<void>> asyncFutures;
        for (const auto& store : partitionSnapshot->Stores) {
            auto reader = store->CreateReader(keys, Timestamp_, ColumnFilter_);
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
        const TPartitionSnapshotPtr partitionSnapshot,
        const TSharedRange<TKey>& keys)
    {
        if (keys.Empty()) {
            return;
        }

        CreateReadSessions(&PartitionSessions_, partitionSnapshot, keys);

        TSchemafulRowMerger merger(
            &MemoryPool_,
            SchemaColumnCount_,
            KeyColumnCount_,
            ColumnFilter_);

        auto processSessions = [&] (TReadSessionList& sessions) {
            for (auto& session : sessions) {
                merger.AddPartialRow(session.FetchRow());
            }
        };

        for (int index = 0; index < keys.Size(); ++index) {
            processSessions(PartitionSessions_);
            processSessions(EdenSessions_);

            auto mergedRow = merger.BuildMergedRow();
            Writer_->WriteUnversionedRow(mergedRow);
        }
    }
};

void LookupRows(
    TTabletSnapshotPtr tabletSnapshot,
    TTimestamp timestamp,
    TWireProtocolReader* reader,
    TWireProtocolWriter* writer)
{
    TLookupSession session(
        tabletSnapshot,
        timestamp,
        reader,
        writer);

    session.Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

#include "lookup.h"
#include "private.h"
#include "store.h"
#include "tablet.h"
#include "tablet_slot.h"

#include <yt/ytlib/table_client/row_merger.h>
#include <yt/ytlib/table_client/versioned_reader.h>

#include <yt/ytlib/tablet_client/wire_protocol.h>
#include <yt/ytlib/tablet_client/wire_protocol.pb.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/nullable.h>
#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/small_vector.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;
static const int BufferCapacity = 1000;

////////////////////////////////////////////////////////////////////////////////

class TLookupSession
{
public:
<<<<<<< HEAD
    TLookupSession()
        : RowBuffer_(New<TRowBuffer>(TRefCountedTypeTag<TLookupPoolTag>()))
        , RunCallback_(BIND(&TLookupSession::DoRun, this))
    {
        Rows_.reserve(BufferCapacity);
    }

    void Prepare(
        TTabletSnapshotPtr tabletSnapshot,
        TTimestamp timestamp,
        TWireProtocolReader* reader)
    {
        Clean();

=======
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
>>>>>>> origin/prestable/0.17.4
        TReqLookupRows req;
        Reader_->ReadMessage(&req);

        TColumnFilter columnFilter;
        if (req.has_column_filter()) {
            columnFilter.All = false;
            columnFilter.Indexes = FromProto<int, SmallVector<int, TypicalColumnCount>>(req.column_filter().indexes());
        }

        ValidateColumnFilter(columnFilter, tabletSnapshot->Schema.Columns().size());

<<<<<<< HEAD
        LookupKeys_ = reader->ReadUnversionedRowset();

        Reader_ = CreateSchemafulTabletReader(
            tabletSnapshot,
            columnFilter,
            LookupKeys_,
            timestamp,
            1,
            RowBuffer_);
    }
=======
        auto schemaData = TWireProtocolReader::GetSchemaData(
            TabletSnapshot_->Schema,
            TabletSnapshot_->KeyColumns.size());
        LookupKeys_ = Reader_->ReadSchemafulRowset(schemaData);
>>>>>>> origin/prestable/0.17.4

        Merger_.Emplace(
            &MemoryPool_,
            SchemaColumnCount_,
            KeyColumnCount_,
            ColumnFilter_);

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

<<<<<<< HEAD
    const TSharedRange<TUnversionedRow>& GetLookupKeys() const
    {
        return LookupKeys_;
    }

    void Clean()
    {
        RowBuffer_->Clear();
        LookupKeys_ = TSharedRange<TUnversionedRow>();
        Reader_.Reset();
        Rows_.clear();
    }

private:
    TRowBufferPtr RowBuffer_;
    TSharedRange<TUnversionedRow> LookupKeys_;
    ISchemafulReaderPtr Reader_;
    std::vector<TUnversionedRow> Rows_;
=======
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
>>>>>>> origin/prestable/0.17.4

    TNullable<TSchemafulRowMerger> Merger_;

<<<<<<< HEAD
    void DoRun(TWireProtocolWriter* writer)
    {
        while (Reader_->Read(&Rows_)) {
            for (const auto& row : Rows_) {
                writer->WriteUnversionedRow(row);
=======

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

        auto processSessions = [&] (TReadSessionList& sessions) {
            for (auto& session : sessions) {
                Merger_->AddPartialRow(session.FetchRow());
>>>>>>> origin/prestable/0.17.4
            }

<<<<<<< HEAD
            if (Rows_.empty()) {
                WaitFor(Reader_->GetReadyEvent())
                    .ThrowOnError();
            }
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
=======
            auto mergedRow = Merger_->BuildMergedRow();
            Writer_->WriteSchemafulRow(mergedRow);
        }
    }
>>>>>>> origin/prestable/0.17.4
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

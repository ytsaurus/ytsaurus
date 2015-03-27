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

#include <ytlib/new_table_client/versioned_reader.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NVersionedTableClient;
using namespace NTabletClient;
using namespace NTabletClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

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
        EdenSessions_.clear();
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

        bool NextRow()
        {
            ++RowIndex_;
            return RowIndex_ < Rows_.size();
        }

        TVersionedRow GetRow() const
        {
            return Rows_[RowIndex_];
        }

        void Refill() 
        {
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

    private:
        static const int BufferCapacity = 1000;
        IVersionedReaderPtr Reader_;
        std::vector<TVersionedRow> Rows_;
        int RowIndex_ = -1;
    };

    TChunkedMemoryPool MemoryPool_;
    std::vector<TUnversionedRow> LookupKeys_;
    std::vector<TReadSession> EdenSessions_;

    TTabletSnapshotPtr TabletSnapshot_;
    TTimestamp Timestamp_;
    int KeyColumnCount_;
    int SchemaColumnCount_;
    TColumnFilter ColumnFilter_;

    TCallback<void(TWireProtocolWriter* writer)> RunCallback_;


    void CreateReadSessions(
        std::vector<TReadSession>* sessions,
        const TPartitionSnapshotPtr partitionSnapshot,
        const std::vector<TKey>& keys)
    {
        sessions->clear();
        if (!partitionSnapshot) {
            return;
        }

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
            WaitFor(Combine(asyncFutures)).ThrowOnError();
        }
    }

    void LookupInPartition(
        const TPartitionSnapshotPtr partitionSnapshot,
        const std::vector<TKey>& keys,
        TWireProtocolWriter* writer)
    {
        if (keys.empty()) {
            return;
        }

        TUnversionedRowMerger merger(
            &MemoryPool_,
            SchemaColumnCount_,
            KeyColumnCount_,
            ColumnFilter_);

        auto processSessions = [&] (std::vector<TReadSession>& sessions) {
            for (auto& session : sessions) {
                if (!session.NextRow()) {
                    session.Refill();
                }

                merger.AddPartialRow(session.GetRow());
            }
        };

        std::vector<TReadSession> sessions;
        CreateReadSessions(&sessions, partitionSnapshot, keys);

        for (int index = 0; index < keys.size(); ++index) {
            processSessions(sessions);
            processSessions(EdenSessions_);

            auto mergedRow = merger.BuildMergedRow();
            writer->WriteUnversionedRow(mergedRow);
        }
    }

    void DoRun(TWireProtocolWriter* writer)
    {
        CreateReadSessions(&EdenSessions_, TabletSnapshot_->Eden, LookupKeys_);

        TPartitionSnapshotPtr currentPartitionSnapshot;
        std::vector<TUnversionedRow> partitionKeys;

        for (auto key : LookupKeys_) {
            ValidateServerKey(key, KeyColumnCount_, TabletSnapshot_->Schema);
            auto partitionSnapshot = TabletSnapshot_->FindContainingPartition(key);
            if (partitionSnapshot != currentPartitionSnapshot) {
                LookupInPartition(currentPartitionSnapshot, partitionKeys, writer);

                currentPartitionSnapshot = std::move(partitionSnapshot);
                partitionKeys.clear();
            }
            partitionKeys.push_back(key);
        }

        LookupInPartition(currentPartitionSnapshot, partitionKeys, writer);
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

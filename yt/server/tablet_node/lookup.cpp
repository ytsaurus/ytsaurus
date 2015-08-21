#include "stdafx.h"
#include "lookup.h"
#include "tablet.h"
#include "store.h"
#include "tablet_slot.h"
#include "private.h"
#include "tablet_reader.h"

#include <core/misc/protobuf_helpers.h>

#include <core/concurrency/scheduler.h>

#include <core/logging/log.h>

#include <ytlib/tablet_client/wire_protocol.h>
#include <ytlib/tablet_client/wire_protocol.pb.h>

#include <ytlib/table_client/schemaful_reader.h>
#include <ytlib/table_client/row_buffer.h>

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

struct TLookupPoolTag { };

class TLookupSession
{
public:
    TLookupSession()
        : RowBuffer_(New<TRowBuffer>(TRefCountedTypeTag<TLookupPoolTag>()))
        , RunCallback_(BIND(&TLookupSession::DoRun, this))
    { }

    void Prepare(
        TTabletSnapshotPtr tabletSnapshot,
        TTimestamp timestamp,
        TWireProtocolReader* reader)
    {
        Clean();

        TReqLookupRows req;
        reader->ReadMessage(&req);

        TColumnFilter columnFilter;
        if (req.has_column_filter()) {
            columnFilter.All = false;
            columnFilter.Indexes = FromProto<int, SmallVector<int, TypicalColumnCount>>(req.column_filter().indexes());
        }

        ValidateColumnFilter(columnFilter, tabletSnapshot->Schema.Columns().size());

        LookupKeys_ = reader->ReadUnversionedRowset();

        Reader_ = CreateSchemafulTabletReader(
            tabletSnapshot,
            columnFilter,
            LookupKeys_,
            timestamp,
            1,
            RowBuffer_);
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
        Rows_.reserve(BufferCapacity);
    }

private:
    TRowBufferPtr RowBuffer_;
    TSharedRange<TUnversionedRow> LookupKeys_;
    ISchemafulReaderPtr Reader_;
    std::vector<TUnversionedRow> Rows_;

    TCallback<void(TWireProtocolWriter* writer)> RunCallback_;

    void DoRun(TWireProtocolWriter* writer)
    {
        while (Reader_->Read(&Rows_)) {
            for (const auto& row : Rows_) {
                writer->WriteUnversionedRow(row);
            }

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
    LOG_DEBUG("Performing tablet lookup (TabletId: %v, CellId: %v, KeyCount: %v, Session: %v)",
        tabletSnapshot->TabletId,
        tabletSnapshot->Slot ? tabletSnapshot->Slot->GetCellId() : NullCellId,
        session->GetLookupKeys().Size(),
        session.get());

    auto resultHolder = session->Run(std::move(poolInvoker), writer);
    return WaitFor(resultHolder.Get())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

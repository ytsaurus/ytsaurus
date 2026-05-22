#include "record_format.h"

#include <yt/yt/ytlib/table_client/schemaless_block_reader.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>
#include <yt/yt/core/misc/serialize.h>

#include <cstring>
#include <memory>

namespace NYT::NPushBasedShuffleClient {

using namespace NCompression;
using namespace NTableClient::NProto;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr i64 HeaderSize = sizeof(TRecordHeader);

const TTableSchemaPtr EmptyTableSchema = New<TTableSchema>();

struct TShufflePayloadTag { };
struct TShuffleWireRecordTag { };

} // namespace

////////////////////////////////////////////////////////////////////////////////

TShuffleRecordBuilder::TShuffleRecordBuilder(i32 mapperId, i64 startRowId)
    : MapperId_(mapperId)
    , NextRowId_(startRowId)
    , BlockWriter_(std::in_place, EmptyTableSchema, GetNullMemoryUsageTracker())
{ }

void TShuffleRecordBuilder::AddRow(TUnversionedRow row)
{
    BlockWriter_->WriteRow(row);
}

std::optional<TShuffleRecord> TShuffleRecordBuilder::FlushRecord()
{
    i64 rowCount = BlockWriter_->GetRowCount();
    if (rowCount == 0) {
        return std::nullopt;
    }

    YT_VERIFY(rowCount <= std::numeric_limits<i32>::max());

    auto block = BlockWriter_->FlushBlock();
    i64 startRow = NextRowId_;
    NextRowId_ += rowCount;
    BlockWriter_.emplace(EmptyTableSchema, GetNullMemoryUsageTracker());

    return TShuffleRecord{
        .Header = TRecordHeader{
            .RowCount = static_cast<i32>(rowCount),
            .MapperId = MapperId_,
            .StartRow = startRow,
        },
        .UncompressedPayload = std::move(block.Data),
    };
}

i64 TShuffleRecordBuilder::GetAllocatedDataSize() const
{
    return BlockWriter_->GetCapacity();
}

////////////////////////////////////////////////////////////////////////////////

TRecordHeader ReadShuffleRecordHeader(TRange<TSharedRef> wire)
{
    TRecordHeader header;
    i64 copied = 0;
    for (const auto& ref : wire) {
        if (copied == HeaderSize) {
            break;
        }
        i64 toCopy = std::min<i64>(ref.Size(), HeaderSize - copied);
        std::memcpy(reinterpret_cast<char*>(&header) + copied, ref.Begin(), toCopy);
        copied += toCopy;
    }
    THROW_ERROR_EXCEPTION_IF(
        copied < HeaderSize,
        "Shuffle record is too short to contain a header: got %v bytes, expected at least %v",
        copied,
        HeaderSize);
    return header;
}

std::vector<TSharedRef> CompressShuffleRecord(
    const TShuffleRecord& record,
    ECodec codec)
{
    auto headerRef = TSharedMutableRef::Allocate<TShuffleWireRecordTag>(
        sizeof(TRecordHeader),
        {.InitializeStorage = false});
    std::memcpy(headerRef.Begin(), &record.Header, sizeof(TRecordHeader));

    auto* codecPtr = GetCodec(codec);
    return {TSharedRef(std::move(headerRef)), codecPtr->Compress(record.UncompressedPayload)};
}

TShuffleRecord DecompressShuffleRecord(
    TRange<TSharedRef> wire,
    ECodec codec)
{
    auto header = ReadShuffleRecordHeader(wire);

    // Build the post-header suffix as a multi-ref. The header occupies
    // HeaderSize bytes from the front; everything else is the codec input.
    // The header may straddle a ref boundary, so the first suffix ref may
    // be a Slice of the ref where the header ends.
    std::vector<TSharedRef> compressedParts;
    i64 skipped = 0;
    for (const auto& ref : wire) {
        i64 refSize = ref.Size();
        if (skipped >= HeaderSize) {
            compressedParts.push_back(ref);
        } else if (skipped + refSize <= HeaderSize) {
            skipped += refSize;
        } else {
            i64 headerBytesHere = HeaderSize - skipped;
            compressedParts.push_back(ref.Slice(headerBytesHere, refSize));
            skipped = HeaderSize;
        }
    }

    auto* codecPtr = GetCodec(codec);
    return TShuffleRecord{
        .Header = header,
        .UncompressedPayload = {codecPtr->Decompress(compressedParts)},
    };
}

TParsedRecord ParseShuffleRecord(
    TShuffleRecord record,
    TChunkedMemoryPool* pool)
{
    THROW_ERROR_EXCEPTION_IF(
        record.Header.RowCount < 0,
        "Shuffle record header has negative row count: %v",
        record.Header.RowCount);

    i32 rowCount = record.Header.RowCount;

    auto payloadRef = record.UncompressedPayload.size() == 1
        ? std::move(record.UncompressedPayload.front())
        : MergeRefsToRef<TShufflePayloadTag>(record.UncompressedPayload);

    if (rowCount == 0) {
        return TParsedRecord{
            .Header = record.Header,
            .UncompressedPayload = std::move(payloadRef),
            .Rows = TRange<TUnversionedRow>(),
        };
    }

    // THorizontalBlockReader only reads Meta.row_count(); other fields are
    // ignored. Set just what's needed.
    TDataBlockMeta meta;
    meta.set_row_count(rowCount);

    THorizontalBlockReader reader(
        payloadRef,
        meta,
        /*compositeColumnFlags*/ {},
        /*hunkColumnFlags*/ {},
        /*hunkChunkRefs*/ nullptr,
        /*hunkChunkMetas*/ nullptr,
        /*chunkToReaderIdMapping*/ {},
        /*sortOrders*/ {},
        /*commonKeyPrefix*/ 0,
        /*keyWideningOptions*/ {},
        /*extraColumnCount*/ 0,
        /*decodeInlineHunkValues*/ false,
        /*unpackAny*/ false);

    auto* rows = reinterpret_cast<TUnversionedRow*>(
        pool->AllocateAligned(sizeof(TUnversionedRow) * rowCount));

    for (i32 i = 0; i < rowCount; ++i) {
        std::construct_at(&rows[i], reader.GetRow(pool, /*remapIds*/ false));
        if (i + 1 < rowCount) {
            YT_VERIFY(reader.NextRow());
        }
    }

    return TParsedRecord{
        .Header = record.Header,
        .UncompressedPayload = std::move(payloadRef),
        .Rows = TRange<TUnversionedRow>(rows, rowCount),
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPushBasedShuffleClient

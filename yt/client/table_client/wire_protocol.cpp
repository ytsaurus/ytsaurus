#include "wire_protocol.h"

#include <yt/client/table_client/proto/chunk_meta.pb.h>
#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/schemaful_reader.h>
#include <yt/client/table_client/schemaful_writer.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/bitmap.h>
#include <yt/core/misc/chunked_memory_pool.h>
#include <yt/core/misc/chunked_output_stream.h>
#include <yt/core/misc/error.h>
#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/serialize.h>

#include <yt/core/compression/codec.h>

#include <util/system/sanitizers.h>

#include <contrib/libs/protobuf/io/coded_stream.h>

namespace NYT::NTableClient {

using NYT::ToProto;
using NYT::FromProto;

using NChunkClient::NProto::TDataStatistics;

////////////////////////////////////////////////////////////////////////////////

struct TWireProtocolWriterTag
{ };

struct TWireProtocolReaderTag
{ };

static constexpr size_t ReaderBufferChunkSize = 4096;

static constexpr size_t WriterInitialBufferCapacity = 1024;
static constexpr size_t PreallocateBlockSize = 4096;

static constexpr ui64 MinusOne = static_cast<ui64>(-1);

static_assert(sizeof(i64) == SerializationAlignment, "Wrong serialization alignment");
static_assert(sizeof(double) == SerializationAlignment, "Wrong serialization alignment");
static_assert(sizeof(TUnversionedValue) == 16, "sizeof(TUnversionedValue) != 16");
static_assert(sizeof(TUnversionedValueData) == 8, "sizeof(TUnversionedValueData) == 8");
static_assert(sizeof(TUnversionedRowHeader) == 8, "sizeof(TUnversionedRowHeader) != 8");
static_assert(sizeof(TVersionedValue) == 24, "sizeof(TVersionedValue) != 24");
static_assert(sizeof(TVersionedRowHeader) == 16, "sizeof(TVersionedRowHeader) != 16");

////////////////////////////////////////////////////////////////////////////////

class TWireProtocolWriter::TImpl
{
public:
    TImpl()
        : Stream_(TWireProtocolWriterTag())
    {
        EnsureCapacity(WriterInitialBufferCapacity);
    }

    size_t GetByteSize() const
    {
        return Stream_.GetSize();
    }

    void WriteCommand(EWireProtocolCommand command)
    {
        WriteUint64(static_cast<unsigned int>(command));
    }

    void WriteTableSchema(const TTableSchema& schema)
    {
        WriteMessage(ToProto<NTableClient::NProto::TTableSchemaExt>(schema));
    }

    void WriteMessage(const ::google::protobuf::MessageLite& message)
    {
        size_t size = static_cast<size_t>(message.ByteSize());
        WriteUint64(size);
        EnsureAlignedUpCapacity(size);
        YCHECK(message.SerializePartialToArray(Current_, size));
        bzero(Current_ + size, AlignUp(size) - size);

        NSan::CheckMemIsInitialized(Current_, AlignUp(size));
        Current_ += AlignUp(size);
    }

    size_t WriteSchemafulRow(
        TUnversionedRow row,
        const TNameTableToSchemaIdMapping* idMapping = nullptr)
    {
        size_t bytes = EstimateSchemafulRowByteSize(row);
        EnsureCapacity(bytes);

        if (!row) {
            UnsafeWriteUint64(MinusOne);
            return bytes;
        }

        UnsafeWriteUint64(row.GetCount());
        UnsafeWriteSchemafulValueRange(TRange<TUnversionedValue>(row.Begin(), row.End()), idMapping);
        return bytes;
    }

    size_t WriteUnversionedRow(
        TUnversionedRow row,
        const TNameTableToSchemaIdMapping* idMapping = nullptr)
    {
        size_t bytes = EstimateUnversionedRowByteSize(row);
        EnsureCapacity(bytes);

        if (!row) {
            UnsafeWriteUint64(MinusOne);
            return bytes;
        }

        UnsafeWriteUint64(row.GetCount());
        UnsafeWriteUnversionedValueRange(TRange<TUnversionedValue>(row.Begin(), row.End()), idMapping);
        return bytes;
    }

    size_t WriteVersionedRow(TVersionedRow row)
    {
        size_t bytes = EstimateVersionedRowByteSize(row);
        EnsureCapacity(bytes);

        if (!row) {
            UnsafeWriteUint64(MinusOne);
            return bytes;
        }

        UnsafeWriteRaw(row.GetHeader(), sizeof(TVersionedRowHeader));
        UnsafeWriteRaw(row.BeginWriteTimestamps(), sizeof(TTimestamp) * row.GetWriteTimestampCount());
        UnsafeWriteRaw(row.BeginDeleteTimestamps(), sizeof(TTimestamp) * row.GetDeleteTimestampCount());

        UnsafeWriteSchemafulValueRange(TRange<TUnversionedValue>(row.BeginKeys(), row.EndKeys()), nullptr);
        UnsafeWriteVersionedValueRange(TRange<TVersionedValue>(row.BeginValues(), row.EndValues()));
        return bytes;
    }

    void WriteUnversionedValueRange(
        TRange<TUnversionedValue> valueRange,
        const TNameTableToSchemaIdMapping* idMapping = nullptr)
    {
        size_t bytes = AlignUp(8); // -1 or value count
        bytes += EstimateUnversionedValueRangeByteSize(valueRange);
        EnsureCapacity(bytes);

        UnsafeWriteUint64(valueRange.Size());
        UnsafeWriteUnversionedValueRange(valueRange, idMapping);
    }

    void WriteUnversionedRowset(
        TRange<TUnversionedRow> rowset,
        const TNameTableToSchemaIdMapping* idMapping = nullptr)
    {
        WriteRowCount(rowset);
        for (auto row : rowset) {
            WriteUnversionedRow(row, idMapping);
        }
    }

    void WriteSchemafulRowset(
        TRange<TUnversionedRow> rowset,
        const TNameTableToSchemaIdMapping* idMapping = nullptr)
    {
        WriteRowCount(rowset);
        for (auto row : rowset) {
            WriteSchemafulRow(row, idMapping);
        }
    }

    void WriteVersionedRowset(
        TRange<TVersionedRow> rowset)
    {
        WriteRowCount(rowset);
        for (auto row : rowset) {
            WriteVersionedRow(row);
        }
    }

    std::vector<TSharedRef> Finish()
    {
        FlushPreallocated();
        return Stream_.Flush();
    }

private:
    TChunkedOutputStream Stream_;
    char* BeginPreallocated_ = nullptr;
    char* EndPreallocated_ = nullptr;
    char* Current_ = nullptr;

    std::vector<TUnversionedValue> PooledValues_;

    void FlushPreallocated()
    {
        if (!Current_) {
            return;
        }

        YCHECK(Current_ <= EndPreallocated_);
        Stream_.Advance(Current_ - BeginPreallocated_);
        BeginPreallocated_ = EndPreallocated_ = Current_ = nullptr;
    }

    void EnsureCapacity(size_t more)
    {
        if (Y_LIKELY(Current_ + more < EndPreallocated_)) {
            return;
        }

        FlushPreallocated();

        size_t size = std::max(PreallocateBlockSize, more);
        Current_ = BeginPreallocated_ = Stream_.Preallocate(size);
        EndPreallocated_ = BeginPreallocated_ + size;
    }

    void EnsureAlignedUpCapacity(size_t more)
    {
        EnsureCapacity(AlignUp(more));
    }

    void UnsafeWriteRaw(const void* buffer, size_t size)
    {
        NSan::CheckMemIsInitialized(buffer, size);

        memcpy(Current_, buffer, size);
        memset(Current_ + size, 0, AlignUp(size) - size);
        
        NSan::CheckMemIsInitialized(Current_, AlignUp(size));
        Current_ += AlignUp(size);
        Y_ASSERT(Current_ <= EndPreallocated_);
    }

    template <class T>
    void UnsafeWritePod(const T& value)
    {
        static_assert(!std::is_reference<T>::value, "T must not be a reference");
        static_assert(!std::is_pointer<T>::value, "T must not be a pointer");
        // Do not use #UnsafeWriteRaw here to allow compiler to optimize memcpy & AlignUp.
        // Both of them are constexprs.
        memcpy(Current_, &value, sizeof(T));

        NSan::CheckMemIsInitialized(Current_, AlignUp(sizeof(T)));
        Current_ += AlignUp(sizeof(T));
        Y_ASSERT(Current_ <= EndPreallocated_);
    }

    void WriteUint64(ui64 value)
    {
        EnsureCapacity(AlignUp(sizeof(ui64)));
        UnsafeWritePod(value);
    }

    void UnsafeWriteUint64(ui64 value)
    {
        UnsafeWritePod(value);
    }

    template <class TRow>
    void WriteRowCount(TRange<TRow> rowset)
    {
        size_t rowCount = rowset.Size();
        ValidateRowCount(rowCount);
        WriteUint64(rowCount);
    }

    void UnsafeWriteSchemafulValue(const TUnversionedValue& value)
    {
        // Write data in-place.
        if (IsStringLikeType(value.Type)) {
            UnsafeWritePod<ui64>(value.Length);
            UnsafeWriteRaw(value.Data.String, value.Length);
        } else if (IsValueType(value.Type)) {
            UnsafeWritePod(value.Data);
        }
    }

    void UnsafeWriteUnversionedValue(const TUnversionedValue& value)
    {
        // Write header (id, type, aggregate, length).
        const ui64* rawValue = reinterpret_cast<const ui64*>(&value);
        UnsafeWritePod<ui64>(rawValue[0]);
        // Write data in-place.
        if (IsStringLikeType(value.Type)) {
            NSan::CheckMemIsInitialized(value.Data.String, value.Length);
            UnsafeWriteRaw(value.Data.String, value.Length);
        } else if (IsValueType(value.Type)) {
            UnsafeWritePod(value.Data);
        }
    }

    void UnsafeWriteVersionedValue(const TVersionedValue& value)
    {
        // Write header (id, type, aggregate, length).
        const ui64* rawValue = reinterpret_cast<const ui64*>(&value);
        UnsafeWritePod<ui64>(rawValue[0]);
        // Write data in-place.
        if (IsStringLikeType(value.Type)) {
            UnsafeWriteRaw(value.Data.String, value.Length);
        } else if (IsValueType(value.Type)) {
            UnsafeWritePod(value.Data);
        }
        // Write timestamp.
        UnsafeWritePod<ui64>(value.Timestamp);
    }

    TRange<TUnversionedValue> RemapValues(
        TRange<TUnversionedValue> values,
        const TNameTableToSchemaIdMapping* idMapping)
    {
        auto valueCount = values.Size();
        PooledValues_.resize(valueCount);
        for (size_t index = 0; index < valueCount; ++index){
            const auto& srcValue = values[index];
            auto& dstValue = PooledValues_[index];
            dstValue = srcValue;
            dstValue.Id = static_cast<ui16>((*idMapping)[srcValue.Id]);
        }

        std::sort(
            PooledValues_.begin(),
            PooledValues_.end(),
            [] (const TUnversionedValue& lhs, const TUnversionedValue& rhs) -> bool {
                return lhs.Id < rhs.Id;
            });

        return MakeRange(PooledValues_);
    }

    void UnsafeWriteNullBitmap(TRange<TUnversionedValue> values)
    {
        auto nullBitmap = TAppendOnlyBitmap<ui64>(values.Size());
        for (int index = 0; index < values.Size(); ++index) {
            nullBitmap.Append(values[index].Type == EValueType::Null);
        }
        UnsafeWriteRaw(nullBitmap.Data(), nullBitmap.Size());
    }

    void UnsafeWriteSchemafulValueRange(
        TRange<TUnversionedValue> values,
        const TNameTableToSchemaIdMapping* idMapping)
    {
        if (idMapping) {
            values = RemapValues(values, idMapping);
        }
        UnsafeWriteNullBitmap(values);
        for (const auto& value : values) {
            UnsafeWriteSchemafulValue(value);
        }
    }

    void UnsafeWriteUnversionedValueRange(
        TRange<TUnversionedValue> values,
        const TNameTableToSchemaIdMapping* idMapping)
    {
        if (idMapping) {
            values = RemapValues(values, idMapping);
        }
        for (const auto& value : values) {
            UnsafeWriteUnversionedValue(value);
        }
    }

    void UnsafeWriteVersionedValueRange(
        TRange<TVersionedValue> values)
    {
        for (const auto& value : values) {
            UnsafeWriteVersionedValue(value);
        }
    }

    size_t EstimateSchemafulValueRangeByteSize(TRange<TUnversionedValue> values)
    {
        size_t bytes = 0;
        bytes += AlignUp(TBitmapTraits<ui64>::GetByteCapacity(values.Size())); // null bitmap
        for (const auto& value : values) {
            if (IsStringLikeType(value.Type)) {
                bytes += AlignUp(8 + value.Length);
            } else if (value.Type != EValueType::Null) {
                bytes += AlignUp(8);
            }
        }
        return bytes;
    }

    size_t EstimateUnversionedValueRangeByteSize(TRange<TUnversionedValue> values)
    {
        size_t bytes = 0;
        for (const auto& value : values) {
            bytes += AlignUp(8);
            if (IsStringLikeType(value.Type)) {
                bytes += AlignUp(value.Length);
            } else if (value.Type != EValueType::Null) {
                bytes += AlignUp(8);
            }
        }
        return bytes;
    }

    size_t EstimateVersionedValueRangeByteSize(TRange<TVersionedValue> values)
    {
        size_t bytes = 0;
        for (const auto& value : values) {
            bytes += AlignUp(16);
            if (IsStringLikeType(value.Type)) {
                bytes += AlignUp(value.Length);
            } else if (value.Type != EValueType::Null) {
                bytes += AlignUp(8);
            }
        }
        return bytes;
    }

    size_t EstimateSchemafulRowByteSize(TUnversionedRow row)
    {
        size_t bytes = AlignUp(8); // -1 or value count
        if (row) {
            bytes += EstimateSchemafulValueRangeByteSize(
                TRange<TUnversionedValue>(row.Begin(), row.GetCount()));
        }
        return bytes;
    }

    size_t EstimateUnversionedRowByteSize(TUnversionedRow row)
    {
        size_t bytes = AlignUp(8); // -1 or value count
        if (row) {
            bytes += EstimateUnversionedValueRangeByteSize(
                TRange<TUnversionedValue>(row.Begin(), row.GetCount()));
        }
        return bytes;
    }

    size_t EstimateVersionedRowByteSize(TVersionedRow row)
    {
        size_t bytes = AlignUp(8); // -1 or value count
        if (row) {
            bytes += AlignUp(8); // -1 or value count
            bytes += AlignUp(sizeof(TTimestamp) * (
                row.GetWriteTimestampCount() +
                row.GetDeleteTimestampCount())); // timestamps
            bytes += EstimateSchemafulValueRangeByteSize(
                TRange<TUnversionedValue>(row.BeginKeys(), row.EndKeys()));
            bytes += EstimateVersionedValueRangeByteSize(
                TRange<TVersionedValue>(row.BeginValues(), row.EndValues()));
        }
        return bytes;
    }
};

////////////////////////////////////////////////////////////////////////////////

TWireProtocolWriter::TWireProtocolWriter()
    : Impl_(std::make_unique<TImpl>())
{ }

TWireProtocolWriter::~TWireProtocolWriter() = default;

size_t TWireProtocolWriter::GetByteSize() const
{
    return Impl_->GetByteSize();
}

std::vector<TSharedRef> TWireProtocolWriter::Finish()
{
    return Impl_->Finish();
}

void TWireProtocolWriter::WriteCommand(EWireProtocolCommand command)
{
    Impl_->WriteCommand(command);
}

void TWireProtocolWriter::WriteTableSchema(const TTableSchema& schema)
{
    Impl_->WriteTableSchema(schema);
}

void TWireProtocolWriter::WriteMessage(const ::google::protobuf::MessageLite& message)
{
    Impl_->WriteMessage(message);
}

size_t TWireProtocolWriter::WriteSchemafulRow(
    TUnversionedRow row,
    const TNameTableToSchemaIdMapping* idMapping)
{
    return Impl_->WriteSchemafulRow(row, idMapping);
}

size_t TWireProtocolWriter::WriteUnversionedRow(
    TUnversionedRow row,
    const TNameTableToSchemaIdMapping* idMapping)
{
    return Impl_->WriteUnversionedRow(row, idMapping);
}

size_t TWireProtocolWriter::WriteVersionedRow(
    TVersionedRow row)
{
    return Impl_->WriteVersionedRow(row);
}

void TWireProtocolWriter::WriteUnversionedValueRange(
    TRange<TUnversionedValue> valueRange,
    const TNameTableToSchemaIdMapping* idMapping)
{
    return Impl_->WriteUnversionedValueRange(valueRange, idMapping);
}

void TWireProtocolWriter::WriteUnversionedRowset(
    TRange<TUnversionedRow> rowset,
    const TNameTableToSchemaIdMapping* idMapping)
{
    Impl_->WriteUnversionedRowset(rowset, idMapping);
}

void TWireProtocolWriter::WriteSchemafulRowset(
    TRange<TUnversionedRow> rowset,
    const TNameTableToSchemaIdMapping* idMapping)
{
    Impl_->WriteSchemafulRowset(rowset, idMapping);
}

void TWireProtocolWriter::WriteVersionedRowset(
    TRange<TVersionedRow> rowset)
{
    Impl_->WriteVersionedRowset(rowset);
}

////////////////////////////////////////////////////////////////////////////////

class TWireProtocolReader::TImpl
{
public:
    explicit TImpl(
        const TSharedRef& data,
        TRowBufferPtr rowBuffer)
        : RowBuffer_(rowBuffer ? rowBuffer : New<TRowBuffer>(TWireProtocolReaderTag(), ReaderBufferChunkSize))
        , Data_(data)
        , Current_(Data_.Begin())
    { }

    const TRowBufferPtr& GetRowBuffer() const
    {
        return RowBuffer_;
    }

    bool IsFinished() const
    {
        return Current_ == Data_.End();
    }

    TIterator GetBegin() const
    {
        return Data_.Begin();
    }

    TIterator GetEnd() const
    {
        return Data_.End();
    }

    TIterator GetCurrent() const
    {
        return Current_;
    }

    void SetCurrent(TIterator current)
    {
        Current_ = current;
    }

    TSharedRef Slice(TIterator begin, TIterator end)
    {
        return Data_.Slice(begin, end);
    }

    EWireProtocolCommand ReadCommand()
    {
        return EWireProtocolCommand(ReadUint64());
    }

    TTableSchema ReadTableSchema()
    {
        NTableClient::NProto::TTableSchemaExt protoSchema;
        ReadMessage(&protoSchema);
        return FromProto<TTableSchema>(protoSchema);
    }

    void ReadMessage(::google::protobuf::MessageLite* message)
    {
        size_t size = ReadUint64();
        ::google::protobuf::io::CodedInputStream chunkStream(
            reinterpret_cast<const ui8*>(Current_),
            static_cast<int>(size));
        message->ParsePartialFromCodedStream(&chunkStream);
        Current_ += AlignUp(size);
    }

    TUnversionedRow ReadSchemafulRow(const TSchemaData& schemaData, bool deep)
    {
        auto valueCount = ReadUint64();
        if (valueCount == MinusOne) {
            return TUnversionedRow();
        }
        ValidateRowValueCount(valueCount);
        auto row = RowBuffer_->AllocateUnversioned(valueCount);
        DoReadSchemafulValueRange(schemaData, deep, row.Begin(), valueCount);
        return row;
    }

    TUnversionedRow ReadUnversionedRow(bool deep)
    {
        auto valueCount = ReadUint64();
        if (valueCount == MinusOne) {
            return TUnversionedRow();
        }
        ValidateRowValueCount(valueCount);
        auto row = RowBuffer_->AllocateUnversioned(valueCount);
        DoReadUnversionedValueRange(deep, row.Begin(), valueCount);
        return row;
    }

    TVersionedRow ReadVersionedRow(const TSchemaData& schemaData, bool deep)
    {
        union
        {
            ui64 parts[2];
            TVersionedRowHeader value;
        } header;

        header.parts[0] = ReadUint64();
        if (header.parts[0] == MinusOne) {
            return TVersionedRow();
        }
        header.parts[1] = ReadUint64();

        ValidateKeyColumnCount(header.value.KeyCount);
        ValidateRowValueCount(header.value.ValueCount);
        ValidateRowValueCount(header.value.WriteTimestampCount);
        ValidateRowValueCount(header.value.DeleteTimestampCount);

        auto row = TMutableVersionedRow::Allocate(
            RowBuffer_->GetPool(),
            header.value.KeyCount,
            header.value.ValueCount,
            header.value.WriteTimestampCount,
            header.value.DeleteTimestampCount);

        ReadRaw(row.BeginWriteTimestamps(), sizeof(TTimestamp) * row.GetWriteTimestampCount());
        ReadRaw(row.BeginDeleteTimestamps(), sizeof(TTimestamp) * row.GetDeleteTimestampCount());

        DoReadSchemafulValueRange(schemaData, deep, row.BeginKeys(), header.value.KeyCount);
        DoReadVersionedValueRange(deep, row.BeginValues(), header.value.ValueCount);

        return row;
    }

    TSharedRange<TUnversionedRow> ReadSchemafulRowset(const TSchemaData& schemaData, bool deep)
    {
        int rowCount = DoReadRowCount();
        auto* rows = RowBuffer_->GetPool()->AllocateUninitialized<TUnversionedRow>(rowCount);
        for (int index = 0; index < rowCount; ++index) {
            rows[index] = ReadSchemafulRow(schemaData, deep);
        }
        return TSharedRange<TUnversionedRow>(rows, rows + rowCount, RowBuffer_);
    }

    TSharedRange<TUnversionedRow> ReadUnversionedRowset(bool deep)
    {
        int rowCount = DoReadRowCount();
        auto* rows = RowBuffer_->GetPool()->AllocateUninitialized<TUnversionedRow>(rowCount);
        for (int index = 0; index < rowCount; ++index) {
            rows[index] = ReadUnversionedRow(deep);
        }
        return TSharedRange<TUnversionedRow>(rows, rows + rowCount, RowBuffer_);
    }

    TSharedRange<TVersionedRow> ReadVersionedRowset(const TSchemaData& schemaData, bool deep)
    {
        int rowCount = DoReadRowCount();
        auto* rows = RowBuffer_->GetPool()->AllocateUninitialized<TVersionedRow>(rowCount);
        for (int index = 0; index < rowCount; ++index) {
            rows[index] = ReadVersionedRow(schemaData, deep);
        }
        return TSharedRange<TVersionedRow>(rows, rows + rowCount, RowBuffer_);
    }

private:
    const TRowBufferPtr RowBuffer_;

    TSharedRef Data_;
    TIterator Current_;

    void ValidateSizeAvailable(size_t size)
    {
        if (Current_ + size > Data_.End()) {
            THROW_ERROR_EXCEPTION("Premature end of stream while reading %v bytes", size);
        }
    }

    void ReadRaw(void* buffer, size_t size)
    {
        ValidateSizeAvailable(size);

        memcpy(buffer, Current_, size);
        Current_ += size;
        Current_ += GetPaddingSize(size);
    }

    const char* PeekRaw(size_t size)
    {
        ValidateSizeAvailable(size);

        auto result = Current_;
        Current_ += size;
        Current_ += GetPaddingSize(size);
        return result;
    }

    template <class T>
    void ReadPod(T* value)
    {
        ValidateSizeAvailable(sizeof(T));

        memcpy(value, Current_, sizeof(T));
        Current_ += sizeof(T);
        Current_ += GetPaddingSize(sizeof(T));
    }

    ui64 ReadUint64()
    {
        ui64 value;
        ReadPod(&value);
        return value;
    }

    ui32 ReadUint32()
    {
        ui64 result = ReadUint64();
        if (result > std::numeric_limits<ui32>::max()) {
            THROW_ERROR_EXCEPTION("Value is out of range to fit into uint32");
        }
        return static_cast<ui32>(result);
    }

    i64 ReadInt64()
    {
        i64 value;
        ReadPod(&value);
        return value;
    }

    i32 ReadInt32()
    {
        i64 result = ReadInt64();
        if (result < std::numeric_limits<i32>::min() || result > std::numeric_limits<i32>::max()) {
            THROW_ERROR_EXCEPTION("Value is out of range to fit into int32");
        }
        return static_cast<i32>(result);
    }

    void DoReadStringData(EValueType type, ui32 length, const char** result, bool deep)
    {
        ui32 limit = 0;
        if (type == EValueType::String) {
            limit = MaxStringValueLength;
        }
        if (type == EValueType::Any) {
            limit = MaxAnyValueLength;
        }
        if (length > limit) {
            THROW_ERROR_EXCEPTION("Value is too long: length %v, limit %v",
                length,
                limit);
        }
        if (deep) {
            char* tmp = RowBuffer_->GetPool()->AllocateUnaligned(length);
            ReadRaw(tmp, length);
            *result = tmp;
        } else {
            *result = PeekRaw(length);
        }
    }

    void DoReadNullBitmap(TReadOnlyBitmap<ui64>* nullBitmap, ui32 count)
    {
        auto* chunks = PeekRaw(TBitmapTraits<ui64>::GetByteCapacity(count));
        nullBitmap->Reset(reinterpret_cast<const ui64*>(chunks), count);
    }

    int DoReadRowCount()
    {
        int rowCount = ReadInt32();
        ValidateRowCount(rowCount);
        return rowCount;
    }

    void DoReadSchemafulValue(
        ui32 schemaData,
        bool null,
        bool deep,
        TUnversionedValue* value)
    {
        ui64* rawValue = reinterpret_cast<ui64*>(value);
        rawValue[0] = schemaData;
        if (null) {
            value->Type = EValueType::Null;
        } else if (IsStringLikeType(value->Type)) {
            value->Length = ReadUint32();
            DoReadStringData(value->Type, value->Length, &value->Data.String, deep);
        } else if (IsValueType(value->Type)) {
            value->Data.Uint64 = ReadUint64();
        }
    }

    void DoReadUnversionedValue(bool deep, TUnversionedValue* value)
    {
        ui64* rawValue = reinterpret_cast<ui64*>(value);
        rawValue[0] = ReadUint64();
        if (IsStringLikeType(value->Type)) {
            DoReadStringData(value->Type, value->Length, &value->Data.String, deep);
        } else if (IsValueType(value->Type)) {
            rawValue[1] = ReadUint64();
        }
    }

    void DoReadVersionedValue(bool deep, TVersionedValue* value)
    {
        ui64* rawValue = reinterpret_cast<ui64*>(value);
        rawValue[0] = ReadUint64();
        if (IsStringLikeType(value->Type)) {
            DoReadStringData(value->Type, value->Length, &value->Data.String, deep);
        } else if (IsValueType(value->Type)) {
            rawValue[1] = ReadUint64();
        }
        value->Timestamp = ReadUint64();
    }

    void DoReadSchemafulValueRange(
        const TSchemaData& schemaData,
        bool deep,
        TUnversionedValue* values,
        ui32 valueCount)
    {
        TReadOnlyBitmap<ui64> nullBitmap;
        DoReadNullBitmap(&nullBitmap, valueCount);
        for (size_t index = 0; index < valueCount; ++index) {
            DoReadSchemafulValue(schemaData[index], nullBitmap[index], deep, &values[index]);
        }
    }

    void DoReadUnversionedValueRange(bool deep, TUnversionedValue* values, ui32 valueCount)
    {
        for (size_t index = 0; index < valueCount; ++index) {
            DoReadUnversionedValue(deep, &values[index]);
        }
    }

    void DoReadVersionedValueRange(bool deep, TVersionedValue* values, ui32 valueCount)
    {
        for (size_t index = 0; index < valueCount; ++index) {
            DoReadVersionedValue(deep, &values[index]);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TWireProtocolReader::TWireProtocolReader(
    const TSharedRef& data,
    TRowBufferPtr rowBuffer)
    : Impl_(std::make_unique<TImpl>(data, std::move(rowBuffer)))
{ }

TWireProtocolReader::~TWireProtocolReader() = default;

const TRowBufferPtr& TWireProtocolReader::GetRowBuffer() const
{
    return Impl_->GetRowBuffer();
}

bool TWireProtocolReader::IsFinished() const
{
    return Impl_->IsFinished();
}

auto TWireProtocolReader::GetBegin() const -> TIterator
{
    return Impl_->GetBegin();
}

auto TWireProtocolReader::GetEnd() const -> TIterator
{
    return Impl_->GetEnd();
}

auto TWireProtocolReader::GetCurrent() const -> TIterator
{
    return Impl_->GetCurrent();
}

void TWireProtocolReader::SetCurrent(TIterator current)
{
    Impl_->SetCurrent(current);
}

TSharedRef TWireProtocolReader::Slice(TIterator begin, TIterator end)
{
    return Impl_->Slice(begin, end);
}

EWireProtocolCommand TWireProtocolReader::ReadCommand()
{
    return Impl_->ReadCommand();
}

TTableSchema TWireProtocolReader::ReadTableSchema()
{
    return Impl_->ReadTableSchema();
}

void TWireProtocolReader::ReadMessage(::google::protobuf::MessageLite* message)
{
    Impl_->ReadMessage(message);
}

TUnversionedRow TWireProtocolReader::ReadUnversionedRow(bool deep)
{
    return Impl_->ReadUnversionedRow(deep);
}

TUnversionedRow TWireProtocolReader::ReadSchemafulRow(const TSchemaData& schemaData, bool deep)
{
    return Impl_->ReadSchemafulRow(schemaData, deep);
}

TVersionedRow TWireProtocolReader::ReadVersionedRow(const TSchemaData& schemaData, bool deep)
{
    return Impl_->ReadVersionedRow(schemaData, deep);
}

TSharedRange<TUnversionedRow> TWireProtocolReader::ReadUnversionedRowset(bool deep)
{
    return Impl_->ReadUnversionedRowset(deep);
}

TSharedRange<TUnversionedRow> TWireProtocolReader::ReadSchemafulRowset(const TSchemaData& schemaData, bool deep)
{
    return Impl_->ReadSchemafulRowset(schemaData, deep);
}

TSharedRange<TVersionedRow> TWireProtocolReader::ReadVersionedRowset(const TSchemaData& schemaData, bool deep)
{
    return Impl_->ReadVersionedRowset(schemaData, deep);
}

auto TWireProtocolReader::GetSchemaData(
    const TTableSchema& schema,
    const TColumnFilter& filter) -> TSchemaData
{
    TSchemaData schemaData;
    auto addColumn = [&] (int id) {
        auto value = MakeUnversionedValueHeader(schema.Columns()[id].GetPhysicalType(), id);
        schemaData.push_back(*reinterpret_cast<ui32*>(&value));
    };
    if (!filter.IsUniversal()) {
        for (int id : filter.GetIndexes()) {
            addColumn(id);
        }
    } else {
        for (int id = 0; id < schema.GetColumnCount(); ++id) {
            addColumn(id);
        }
    }
    return schemaData;
}

auto TWireProtocolReader::GetSchemaData(const TTableSchema& schema) -> TSchemaData
{
    TSchemaData schemaData;
    for (int id = 0; id < schema.GetColumnCount(); ++id) {
        auto value = MakeUnversionedValueHeader(schema.Columns()[id].GetPhysicalType(), id);
        schemaData.push_back(*reinterpret_cast<ui32*>(&value));
    }
    return schemaData;
}

////////////////////////////////////////////////////////////////////////////////

class TWireProtocolRowsetReader
    : public IWireProtocolRowsetReader
{
public:
    TWireProtocolRowsetReader(
        const std::vector<TSharedRef>& compressedBlocks,
        NCompression::ECodec codecId,
        const TTableSchema& schema,
        bool isSchemaful,
        const NLogging::TLogger& logger)
        : CompressedBlocks_(compressedBlocks)
        , Codec_(NCompression::GetCodec(codecId))
        , Schema_(schema)
        , IsSchemaful(isSchemaful)
        , Logger(
            NLogging::TLogger(logger)
                .AddTag("ReaderId: %v", TGuid::Create()))
    {
        LOG_DEBUG("Wire protocol rowset reader created (BlockCount: %v, TotalCompressedSize: %v)",
            CompressedBlocks_.size(),
            GetByteSize(CompressedBlocks_));
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        if (Finished_) {
            return false;
        }

        if (BlockIndex_ >= CompressedBlocks_.size()) {
            Finished_ = true;
            LOG_DEBUG("Wire protocol rowset reader finished");
            return false;
        }

        const auto& compressedBlock = CompressedBlocks_[BlockIndex_];
        LOG_DEBUG("Started decompressing rowset reader block (BlockIndex: %v, CompressedSize: %v)",
            BlockIndex_,
            compressedBlock.Size());
        auto uncompressedBlock = Codec_->Decompress(compressedBlock);
        LOG_DEBUG("Finished decompressing rowset reader block (BlockIndex: %v, UncompressedSize: %v)",
            BlockIndex_,
            uncompressedBlock.Size());

        auto rowBuffer = New<TRowBuffer>(TWireProtocolReaderTag(), ReaderBufferChunkSize);
        WireReader_ = std::make_unique<TWireProtocolReader>(uncompressedBlock, std::move(rowBuffer));

        if (!SchemaChecked_) {
            auto actualSchema = WireReader_->ReadTableSchema();
            if (Schema_ != actualSchema) {
                THROW_ERROR_EXCEPTION("Schema mismatch while parsing wire protocol");
            }
            SchemaChecked_ = true;
        }

        auto schemaData = WireReader_->GetSchemaData(Schema_, TColumnFilter());

        rows->clear();
        while (!WireReader_->IsFinished()) {
            auto row = IsSchemaful
                ? WireReader_->ReadSchemafulRow(schemaData, false)
                : WireReader_->ReadUnversionedRow(false);
            rows->push_back(row);
        }
        ++BlockIndex_;

        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return VoidFuture;
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        Y_UNREACHABLE();
    }

    virtual NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        Y_UNIMPLEMENTED();
    }

private:
    const std::vector<TSharedRef> CompressedBlocks_;
    NCompression::ICodec* const Codec_;
    const TTableSchema Schema_;
    bool IsSchemaful;
    const NLogging::TLogger Logger;

    int BlockIndex_ = 0;
    std::unique_ptr<TWireProtocolReader> WireReader_;
    bool Finished_ = false;
    bool SchemaChecked_ = false;

};

IWireProtocolRowsetReaderPtr CreateWireProtocolRowsetReader(
    const std::vector<TSharedRef>& compressedBlocks,
    NCompression::ECodec codecId,
    const TTableSchema& schema,
    bool isSchemaful,
    const NLogging::TLogger& logger)
{
    return New<TWireProtocolRowsetReader>(
        compressedBlocks,
        codecId,
        schema,
        isSchemaful,
        logger);
}

////////////////////////////////////////////////////////////////////////////////

class TWireProtocolRowsetWriter
    : public IWireProtocolRowsetWriter
{
public:
    TWireProtocolRowsetWriter(
        NCompression::ECodec codecId,
        size_t desiredUncompressedBlockSize,
        const TTableSchema& schema,
        bool isSchemaful,
        const NLogging::TLogger& logger)
        : Codec_(NCompression::GetCodec(codecId))
        , DesiredUncompressedBlockSize_(desiredUncompressedBlockSize)
        , Schema_(schema)
        , IsSchemaful(isSchemaful)
        , Logger(NLogging::TLogger(logger)
            .AddTag("WriterId: %v", TGuid::Create()))
    {
        LOG_DEBUG("Wire protocol rowset writer created (CodecId: %v, DesiredUncompressedBlockSize: %v)",
            codecId,
            DesiredUncompressedBlockSize_);
    }

    virtual TFuture<void> Close() override
    {
        if (!Closed_) {
            LOG_DEBUG("Wire protocol rowset writer closed");
            FlushBlock();
            Closed_ = true;
        }
        return VoidFuture;
    }

    virtual bool Write(TRange<TUnversionedRow> rows) override
    {
        YCHECK(!Closed_);
        for (auto row : rows) {
            if (!WireWriter_) {
                WireWriter_ = std::make_unique<TWireProtocolWriter>();
                if (!SchemaWritten_) {
                    WireWriter_->WriteTableSchema(Schema_);
                    SchemaWritten_ = true;
                }
            }
            if (IsSchemaful) {
                WireWriter_->WriteSchemafulRow(row);
            } else {
                WireWriter_->WriteUnversionedRow(row);
            }
            if (WireWriter_->GetByteSize() >= DesiredUncompressedBlockSize_) {
                FlushBlock();
            }
        }
        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return VoidFuture;
    }

    virtual std::vector<TSharedRef> GetCompressedBlocks() override
    {
        YCHECK(Closed_);
        return CompressedBlocks_;
    }

private:
    NCompression::ICodec* const Codec_;
    const size_t DesiredUncompressedBlockSize_;
    const TTableSchema Schema_;
    bool IsSchemaful;
    const NLogging::TLogger Logger;

    std::vector<TSharedRef> CompressedBlocks_;
    std::unique_ptr<TWireProtocolWriter> WireWriter_;
    bool Closed_ = false;
    bool SchemaWritten_ = false;


    void FlushBlock()
    {
        if (!WireWriter_) {
            return;
        }

        auto uncompressedBlocks = WireWriter_->Finish();

        LOG_DEBUG("Started compressing rowset writer block (BlockIndex: %v, UncompressedSize: %v)",
            CompressedBlocks_.size(),
            GetByteSize(uncompressedBlocks));
        auto compressedBlock = Codec_->Compress(uncompressedBlocks);
        LOG_DEBUG("Finished compressing rowset writer block (BlockIndex: %v, CompressedSize: %v)",
            CompressedBlocks_.size(),
            compressedBlock.Size());

        CompressedBlocks_.push_back(compressedBlock);
        WireWriter_.reset();
    }
};

IWireProtocolRowsetWriterPtr CreateWireProtocolRowsetWriter(
    NCompression::ECodec codecId,
    size_t desiredUncompressedBlockSize,
    const NTableClient::TTableSchema& schema,
    bool isSchemaful,
    const NLogging::TLogger& logger)
{
    return New<TWireProtocolRowsetWriter>(
        codecId,
        desiredUncompressedBlockSize,
        schema,
        isSchemaful,
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient


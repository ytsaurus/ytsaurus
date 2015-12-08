#include "wire_protocol.h"

#include <yt/ytlib/table_client/chunk_meta.pb.h>
#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/schemaful_reader.h>
#include <yt/ytlib/table_client/schemaful_writer.h>
#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/bitmap.h>
#include <yt/core/misc/chunked_memory_pool.h>
#include <yt/core/misc/chunked_output_stream.h>
#include <yt/core/misc/error.h>
#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/serialize.h>

#include <contrib/libs/protobuf/io/coded_stream.h>

namespace NYT {
namespace NTabletClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

struct TWireProtocolReaderPoolTag { };
static const size_t ReaderChunkSize = 16384;

struct TWireProtocolWriterChunkTag { };
static const size_t WriterInitialBufferCapacity = 1024;
static const size_t PreallocateBlockSize = 4096;

static_assert(sizeof (i64) == SerializationAlignment, "Wrong serialization alignment");
static_assert(sizeof (double) == SerializationAlignment, "Wrong serialization alignment");
static_assert(sizeof (TUnversionedValue) == 2 * sizeof (i64), "Wrong TUnversionedValue size");

////////////////////////////////////////////////////////////////////////////////

class TWireProtocolWriter::TImpl
    : public TIntrinsicRefCounted
{
public:
    TImpl()
        : Stream_(TWireProtocolWriterChunkTag())
    {
        EnsureCapacity(WriterInitialBufferCapacity);
    }

    void WriteCommand(EWireProtocolCommand command)
    {
        WriteInt64(static_cast<int>(command));
    }

    void WriteTableSchema(const TTableSchema& schema)
    {
        WriteMessage(ToProto< NTableClient::NProto::TTableSchemaExt>(schema));
    }

    void WriteMessage(const ::google::protobuf::MessageLite& message)
    {
        int size = message.ByteSize();
        WriteInt64(size);
        EnsureCapacity(size);
        YCHECK(message.SerializePartialToArray(Current_, size));
        Current_ += AlignUp(size);
    }

    void WriteSchemafulRow(
        TUnversionedRow row,
        const TNameTableToSchemaIdMapping* idMapping = nullptr)
    {
        if (row) {
            WriteRowValues<true>(TRange<TUnversionedValue>(row.Begin(), row.End()), idMapping);
        } else {
            WriteRowValues<true>(TRange<TUnversionedValue>(), idMapping);
        }
    }

    void WriteUnversionedRow(
        TUnversionedRow row,
        const TNameTableToSchemaIdMapping* idMapping = nullptr)
    {
        if (row) {
            WriteRowValues<false>(TRange<TUnversionedValue>(row.Begin(), row.End()), idMapping);
        } else {
            WriteRowValues<false>(TRange<TUnversionedValue>(), idMapping);
        }
    }

    void WriteUnversionedRow(
        const TRange<TUnversionedValue>& values,
        const TNameTableToSchemaIdMapping* idMapping = nullptr)
    {
        WriteRowValues<false>(values, idMapping);
    }

    void WriteUnversionedRowset(
        const TRange<TUnversionedRow>& rowset,
        const TNameTableToSchemaIdMapping* idMapping = nullptr)
    {
        WriteRowCount(rowset);
        for (auto row : rowset) {
            WriteUnversionedRow(row, idMapping);
        }
    }

    void WriteSchemafulRowset(
        const TRange<TUnversionedRow>& rowset,
        const TNameTableToSchemaIdMapping* idMapping = nullptr)
    {
        WriteRowCount(rowset);
        for (auto row : rowset) {
            WriteSchemafulRow(row, idMapping);
        }
    }

    std::vector<TSharedRef> Flush()
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
        if (!Current_)
            return;

        YCHECK(Current_ <= EndPreallocated_);
        Stream_.Advance(Current_ - BeginPreallocated_);
        BeginPreallocated_ = EndPreallocated_ = Current_ = nullptr;
    }

    void EnsureCapacity(size_t more)
    {
        if (Y_LIKELY(Current_ + more < EndPreallocated_))
            return;

        FlushPreallocated();
        size_t size = std::max(PreallocateBlockSize, more);
        Current_ = BeginPreallocated_ = Stream_.Preallocate(size);
        EndPreallocated_ = BeginPreallocated_ + size;
    }


    void UnsafeWriteInt64(i64 value)
    {
        *reinterpret_cast<i64*>(Current_) = value;
        Current_ += sizeof (i64);
    }

    void WriteInt64(i64 value)
    {
        EnsureCapacity(sizeof (i64));
        UnsafeWriteInt64(value);
    }

    void UnsafeWriteRaw(const void* buffer, size_t size)
    {
        memcpy(Current_, buffer, size);
        Current_ += AlignUp(size);
    }

    void WriteRaw(const void* buffer, size_t size)
    {
        EnsureCapacity(size + SerializationAlignment);
        UnsafeWriteRaw(buffer, size);
    }


    void WriteString(const Stroka& value)
    {
        WriteInt64(value.length());
        WriteRaw(value.begin(), value.length());
    }

    void WriteRowCount(const TRange<TUnversionedRow>& rowset)
    {
        int rowCount = static_cast<int>(rowset.Size());
        ValidateRowCount(rowCount);
        WriteInt64(rowCount);
    }

    template <bool Schemaful>
    void WriteRowValue(const TUnversionedValue& value)
    {
        // This includes the value itself and possible serialization alignment.
<<<<<<< HEAD
        i64 bytes = 2 * sizeof (i64);
        if (IsStringLikeType(value.Type)) {
            bytes += value.Length;
=======
        i64 bytes = (Schemaful ? 1 : 2) * sizeof (i64);
        if (IsStringLikeType(EValueType(value.Type))) {
            bytes += value.Length + (Schemaful ? sizeof (i64) : 0);
>>>>>>> origin/prestable/0.17.4
        }
        EnsureCapacity(bytes);

        const i64* rawValue = reinterpret_cast<const i64*>(&value);
        if (!Schemaful) {
            UnsafeWriteInt64(rawValue[0]);
        }
        switch (value.Type) {
            case EValueType::Int64:
            case EValueType::Uint64:
            case EValueType::Double:
            case EValueType::Boolean:
                UnsafeWriteInt64(rawValue[1]);
                break;

            case EValueType::String:
            case EValueType::Any:
                if (Schemaful) {
                    UnsafeWriteInt64(value.Length);
                }
                UnsafeWriteRaw(value.Data.String, value.Length);
                break;

            default:
                break;
        }
    }

    template <bool Schemaful>
    void WriteNullVector(const TRange<TUnversionedValue>& values)
    {
        if (Schemaful) {
            auto nullBitmap = TAppendOnlyBitmap<ui64>(values.Size());
            for (int index = 0; index < values.Size(); ++index) {
                nullBitmap.Append(values[index].Type == EValueType::Null);
            }
            WriteRaw(nullBitmap.Data(), nullBitmap.Size());
        }
    }

    template <bool Schemaful>
    void WriteRowValues(
        const TRange<TUnversionedValue>& values,
        const TNameTableToSchemaIdMapping* idMapping)
    {
        if (!values) {
            WriteInt64(-1);
            return;
        }

        int valueCount = values.Size();
        WriteInt64(valueCount);

        if (idMapping) {
            PooledValues_.resize(valueCount);
            for (int index = 0; index < valueCount; ++index) {
                const auto& srcValue = values[index];
                auto& dstValue = PooledValues_[index];
                dstValue = srcValue;
                dstValue.Id = (*idMapping)[srcValue.Id];
            }

            std::sort(
                PooledValues_.begin(),
                PooledValues_.end(),
                [] (const TUnversionedValue& lhs, const TUnversionedValue& rhs) {
                    return lhs.Id < rhs.Id;
                });

            WriteNullVector<Schemaful>(
                TRange<TUnversionedValue>(PooledValues_.data(), PooledValues_.size()));
            for (int index = 0; index < valueCount; ++index) {
                WriteRowValue<Schemaful>(PooledValues_[index]);
            }
        } else {
            WriteNullVector<Schemaful>(values);
            for (const auto& value : values) {
                WriteRowValue<Schemaful>(value);
            }
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

class TWireProtocolWriter::TSchemafulRowsetWriter
    : public ISchemafulWriter
{
public:
    explicit TSchemafulRowsetWriter(TWireProtocolWriter::TImplPtr writer)
        : Writer_(std::move(writer))
    { }

    virtual TFuture<void> Close() override
    {
        Writer_->WriteCommand(EWireProtocolCommand::EndOfRowset);
        return VoidFuture;
    }

    virtual bool Write(const std::vector<TUnversionedRow>& rows) override
    {
        Writer_->WriteCommand(EWireProtocolCommand::RowsetChunk);
        Writer_->WriteUnversionedRowset(rows);
        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return VoidFuture;
    }

private:
    const TWireProtocolWriter::TImplPtr Writer_;

};

////////////////////////////////////////////////////////////////////////////////

TWireProtocolWriter::TWireProtocolWriter()
    : Impl_(New<TImpl>())
{ }

TWireProtocolWriter::~TWireProtocolWriter()
{ }

std::vector<TSharedRef> TWireProtocolWriter::Flush()
{
    return Impl_->Flush();
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

void TWireProtocolWriter::WriteSchemafulRow(
    TUnversionedRow row,
    const TNameTableToSchemaIdMapping* idMapping)
{
    Impl_->WriteSchemafulRow(row, idMapping);
}

void TWireProtocolWriter::WriteUnversionedRow(
    TUnversionedRow row,
    const TNameTableToSchemaIdMapping* idMapping)
{
    Impl_->WriteUnversionedRow(row, idMapping);
}

void TWireProtocolWriter::WriteUnversionedRow(
    const TRange<TUnversionedValue>& row,
    const TNameTableToSchemaIdMapping* idMapping)
{
    Impl_->WriteUnversionedRow(row, idMapping);
}

void TWireProtocolWriter::WriteUnversionedRowset(
    const TRange<TUnversionedRow>& rowset,
    const TNameTableToSchemaIdMapping* idMapping)
{
    Impl_->WriteUnversionedRowset(rowset, idMapping);
}

void TWireProtocolWriter::WriteSchemafulRowset(
    const TRange<TUnversionedRow>& rowset,
    const TNameTableToSchemaIdMapping* idMapping)
{
    Impl_->WriteSchemafulRowset(rowset, idMapping);
}

ISchemafulWriterPtr TWireProtocolWriter::CreateSchemafulRowsetWriter(
    const NTableClient::TTableSchema& schema)
{
    WriteTableSchema(schema);
    return New<TSchemafulRowsetWriter>(Impl_);
}

////////////////////////////////////////////////////////////////////////////////

class TWireProtocolReader::TImpl
    : public TIntrinsicRefCounted
{
public:
    explicit TImpl(const TSharedRef& data)
        : Data_(data)
        , Current_(Data_.Begin())
    { }

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
        return EWireProtocolCommand(ReadInt64());
    }

    TTableSchema ReadTableSchema()
    {
        NTableClient::NProto::TTableSchemaExt protoSchema;
        ReadMessage(&protoSchema);
        return FromProto<TTableSchema>(protoSchema);
    }

    void ReadMessage(::google::protobuf::MessageLite* message)
    {
        i64 size = ReadInt64();
        ::google::protobuf::io::CodedInputStream chunkStream(
            reinterpret_cast<const ui8*>(Current_),
            size);
        message->ParsePartialFromCodedStream(&chunkStream);
        Current_ += AlignUp(size);
    }

    TUnversionedRow ReadSchemafulRow(const TSchemaData& schemaData)
    {
        return ReadRow<true>(&schemaData);
    }

    TUnversionedRow ReadUnversionedRow()
    {
        return ReadRow<false>();
    }

    TSharedRange<TUnversionedRow> ReadUnversionedRowset()
    {
        return ReadRowset<false>();
    }

    TSharedRange<TUnversionedRow> ReadSchemafulRowset(const TSchemaData& schemaData)
    {
        return ReadRowset<true>(&schemaData);
    }

private:
    TSharedRef Data_;
    TIterator Current_;
    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>(
        ReaderChunkSize,
        TChunkedMemoryPool::DefaultMaxSmallBlockSizeRatio,
        GetRefCountedTypeCookie<TWireProtocolReaderPoolTag>());


    i64 ReadInt64()
    {
        YCHECK(Current_ + sizeof(i64) <= Data_.End());
        i64 result = *reinterpret_cast<const i64*>(Current_);
        Current_ += sizeof(result);
        return result;
    }

    i32 ReadInt32()
    {
        i64 result = ReadInt64();
        if (result > std::numeric_limits<i32>::max()) {
            THROW_ERROR_EXCEPTION("Value is too big to fit into int32");
        }
        return static_cast<i32>(result);
    }

    void Skip(size_t size)
    {
        YCHECK(Current_ + size <= Data_.End());
        Current_ += size;
        Current_ += GetPaddingSize(size);
    }

    void ReadRaw(void* buffer, size_t size)
    {
        YCHECK(Current_ + size <= Data_.End());
        memcpy(buffer, Current_, size);
        Skip(size);
    }


    Stroka ReadString()
    {
        size_t length = ReadInt64();
        Stroka value;
        value.resize(length);
        ReadRaw(const_cast<char*>(value.data()), length);
        return value;
    }

    template <bool Schemaful>
    void ReadRowValue(
        TUnversionedValue* value,
        const TSchemaData* schemaData,
        const TReadOnlyBitmap<ui64>& nullBitmap,
        int index)
    {
        i64* rawValue = reinterpret_cast<i64*>(value);
        if (Schemaful) {
            rawValue[0] = (*schemaData)[index];
            if (nullBitmap[index]) {
                value->Type = EValueType::Null;
            }
        } else {
            rawValue[0] = ReadInt64();
        }

        switch (value->Type) {
            case EValueType::Int64:
            case EValueType::Uint64:
            case EValueType::Double:
            case EValueType::Boolean:
                rawValue[1] = ReadInt64();
                break;

            case EValueType::String:
            case EValueType::Any:
                if (Schemaful) {
                    value->Length = ReadInt32();
                }
                if (value->Length > MaxStringValueLength) {
                    THROW_ERROR_EXCEPTION("Value is too long: length %v, limit %v",
                        value->Length,
                        MaxStringValueLength);
                }
                value->Data.String = RowBuffer_->GetPool()->AllocateUnaligned(value->Length);
                ReadRaw(const_cast<char*>(value->Data.String), value->Length);
                break;

            default:
                break;
        }
    }

    template <bool Schemaful>
    TUnversionedRow ReadRow(const TSchemaData* schemaData = nullptr)
    {
        int valueCount = ReadInt32();
        if (valueCount == -1) {
            return TUnversionedRow();
        }

        ValidateRowValueCount(valueCount);

<<<<<<< HEAD
        auto row = TMutableUnversionedRow::Allocate(RowBuffer_->GetPool(), valueCount);
=======
        auto nullBitmap = TReadOnlyBitmap<ui64>();
        if (schemaData) {
            nullBitmap.Reset(reinterpret_cast<const ui64*>(Current_), valueCount);
            Skip(nullBitmap.GetByteSize());
        }

        auto row = TUnversionedRow::Allocate(RowBuffer_->GetPool(), valueCount);
>>>>>>> origin/prestable/0.17.4
        for (int index = 0; index < valueCount; ++index) {
            ReadRowValue<Schemaful>(&row[index], schemaData, nullBitmap, index);
        }
        return row;
    }

    template <bool Schemaful>
    TSharedRange<TUnversionedRow> ReadRowset(const TSchemaData* schemaData = nullptr)
    {
        int rowCount = ReadInt32();
        ValidateRowCount(rowCount);

        auto* rows = RowBuffer_->GetPool()->AllocateUninitialized<TUnversionedRow>(rowCount);
        for (int index = 0; index != rowCount; ++index) {
            rows[index] = ReadRow<Schemaful>(schemaData);
        }

        return TSharedRange<TUnversionedRow>(rows, rows + rowCount, RowBuffer_);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TWireProtocolReader::TSchemafulRowsetReader
    : public ISchemafulReader
{
public:
    explicit TSchemafulRowsetReader(TWireProtocolReader::TImplPtr reader)
        : Reader_(std::move(reader))
    { }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        if (Finished_) {
            return false;
        }

        while (true) {
            auto command = Reader_->ReadCommand();
            if (command == EWireProtocolCommand::EndOfRowset)
                break;
            YCHECK(command == EWireProtocolCommand::RowsetChunk);

            // TODO(babenko): fixme
            auto sharedRows = Reader_->ReadUnversionedRowset();
            rows->reserve(rows->size() + sharedRows.Size());
            for (auto row : sharedRows) {
                rows->push_back(row);
            }
        }
        Finished_ = true;
        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return VoidFuture;
    }

private:
    const TIntrusivePtr<TWireProtocolReader::TImpl> Reader_;
    bool Finished_ = false;

};

////////////////////////////////////////////////////////////////////////////////

TWireProtocolReader::TWireProtocolReader(const TSharedRef& data)
    : Impl_(New<TImpl>(data))
{ }

TWireProtocolReader::~TWireProtocolReader()
{ }

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

TUnversionedRow TWireProtocolReader::ReadUnversionedRow()
{
    return Impl_->ReadUnversionedRow();
}

TUnversionedRow TWireProtocolReader::ReadSchemafulRow(const TSchemaData& schemaData)
{
    return Impl_->ReadSchemafulRow(schemaData);
}

TSharedRange<TUnversionedRow> TWireProtocolReader::ReadUnversionedRowset()
{
    return Impl_->ReadUnversionedRowset();
}

TSharedRange<TUnversionedRow> TWireProtocolReader::ReadSchemafulRowset(const TSchemaData& schemaData)
{
    return Impl_->ReadSchemafulRowset(schemaData);
}

ISchemafulReaderPtr TWireProtocolReader::CreateSchemafulRowsetReader(const TTableSchema& schema)
{
    auto actualSchema = Impl_->ReadTableSchema();
    if (schema != actualSchema) {
        THROW_ERROR_EXCEPTION("Schema mismatch while parsing wire protocol");
    }
    return New<TSchemafulRowsetReader>(Impl_);
}

auto TWireProtocolReader::GetSchemaData(
    const TTableSchema& schema,
    const TColumnFilter& filter) -> TSchemaData
{
    TSchemaData schemaData;
    auto addColumn = [&] (int id) {
        TUnversionedValue value;
        value.Id = id;
        value.Type = schema.Columns()[id].Type;
        schemaData.push_back(*reinterpret_cast<ui32*>(&value));
    };

    if (!filter.All) {
        for (int id : filter.Indexes) {
            addColumn(id);
        }
    } else {
        for (int id = 0; id < schema.Columns().size(); ++id) {
            addColumn(id);
        }
    }
    return schemaData;
}

auto TWireProtocolReader::GetSchemaData(
    const TTableSchema& schema,
    int keyColumnCount) -> TSchemaData
{
    TSchemaData schemaData;
    //TODO(savrus) merge with new schema.
    YCHECK(keyColumnCount <= schema.Columns().size());
    for (int id = 0; id < keyColumnCount; ++id) {
        TUnversionedValue value;
        value.Id = id;
        value.Type = schema.Columns()[id].Type;
        schemaData.push_back(*reinterpret_cast<ui32*>(&value));
    }
    return schemaData;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT


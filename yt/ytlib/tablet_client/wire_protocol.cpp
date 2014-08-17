#include "stdafx.h"
#include "wire_protocol.h"

#include <core/actions/future.h>

#include <core/misc/error.h>
#include <core/misc/chunked_memory_pool.h>
#include <core/misc/chunked_output_stream.h>
#include <core/misc/serialize.h>
#include <core/misc/protobuf_helpers.h>

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/schemaful_reader.h>
#include <ytlib/new_table_client/schemaful_writer.h>
#include <ytlib/new_table_client/chunk_meta.pb.h>

namespace NYT {
namespace NTabletClient {

using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

static const i64 CurrentProtocolVersion = 2;

static const size_t ReaderAlignedChunkSize = 16384;
static const size_t ReaderUnalignedChunkSize = 16384;

static const size_t WriterInitialBufferCapacity = 1024;

static_assert(sizeof (i64) == SerializationAlignment, "Wrong serialization alignment");
static_assert(sizeof (double) == SerializationAlignment, "Wrong serialization alignment");
static_assert(sizeof (TUnversionedValue) == 2 * sizeof (i64), "Wrong TUnversionedValue size");

////////////////////////////////////////////////////////////////////////////////

class TWireProtocolWriter::TSchemafulRowsetWriter
    : public ISchemafulWriter
{
public:
    explicit TSchemafulRowsetWriter(TWireProtocolWriter* writer)
        : Writer_(writer)
    { }

    virtual TAsyncError Open(
        const TTableSchema& schema,
        const TNullable<TKeyColumns>& /*keyColumns*/) override
    {
        Writer_->WriteTableSchema(schema);
        return OKFuture;
    }

    virtual TAsyncError Close() override
    {
        Writer_->WriteCommand(EWireProtocolCommand::EndOfRowset);
        return OKFuture;
    }

    virtual bool Write(const std::vector<TUnversionedRow>& rows) override
    {
        Writer_->WriteCommand(EWireProtocolCommand::RowsetChunk);
        Writer_->WriteUnversionedRowset(rows);
        return true;
    }

    virtual TAsyncError GetReadyEvent() override
    {
        return OKFuture;
    }

private:
    TWireProtocolWriter* Writer_;

};

////////////////////////////////////////////////////////////////////////////////

struct TWireProtocolWriterChunkTag { };
static const size_t PreallocateBlockSize = 4096;

class TWireProtocolWriter::TImpl
{
public:
    TImpl()
        : Stream_(TWireProtocolWriterChunkTag())
        , BeginPreallocated_(nullptr)
        , EndPreallocated_(nullptr)
        , Current_(nullptr)
    {
        EnsureCapacity(WriterInitialBufferCapacity);
        WriteInt64(CurrentProtocolVersion);
    }

    void WriteCommand(EWireProtocolCommand command)
    {
        WriteInt64(command);
    }

    void WriteColumnFilter(const TColumnFilter& filter)
    {
        if (filter.All) {
            WriteInt64(-1);
        } else {
            WriteInt64(filter.Indexes.size());
            for (int index : filter.Indexes) {
                WriteInt64(index);
            }
        }
    }

    void WriteTableSchema(const TTableSchema& schema)
    {
        WriteMessage(ToProto<NVersionedTableClient::NProto::TTableSchemaExt>(schema));
    }

    void WriteMessage(const ::google::protobuf::MessageLite& message)
    {
        int size = message.ByteSize();
        WriteInt64(size);
        EnsureCapacity(size);
        YCHECK(message.SerializePartialToArray(Current_, size));
        Current_ += AlignUp(size);
    }

    void WriteUnversionedRow(
        TUnversionedRow row,
        const TNameTableToSchemaIdMapping* idMapping)
    {
        if (row) {
            WriteRowValues(row.Begin(), row.End(), idMapping);
        } else {
            WriteRowValues(nullptr, nullptr, idMapping);
        }
    }

    void WriteUnversionedRow(
        const std::vector<TUnversionedValue>& row,
        const TNameTableToSchemaIdMapping* idMapping)
    {
        WriteRowValues(row.data(), row.data() + row.size(), idMapping);
    }

    void WriteUnversionedRowset(
        const std::vector<TUnversionedRow>& rowset,
        const TNameTableToSchemaIdMapping* idMapping)
    {
        int rowCount = static_cast<int>(rowset.size());
        ValidateRowCount(rowCount);
        WriteInt64(rowCount);
        for (auto row : rowset) {
            WriteUnversionedRow(row, idMapping);
        }
    }

    std::vector<TSharedRef> Flush()
    {
        FlushPreallocated();
        return Stream_.Flush();
    }

private:
    TChunkedOutputStream Stream_;
    char* BeginPreallocated_;
    char* EndPreallocated_;
    char* Current_;

    std::vector<TUnversionedValue> PooledValues_;


    void FlushPreallocated()
    {
        if (!Current_)
            return;

        Stream_.Advance(Current_ - BeginPreallocated_);
        BeginPreallocated_ = EndPreallocated_ = Current_ = nullptr;
    }

    void EnsureCapacity(size_t more)
    {
        if (LIKELY(Current_ + more < EndPreallocated_))
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

    void WriteRowValue(const TUnversionedValue& value)
    {
        // This includes value and possible serialization alignment.
        i64 bytes = 2 * sizeof (i64);
        if (value.Type == EValueType::String || value.Type == EValueType::Any) {
            bytes += value.Length;
        }
        EnsureCapacity(bytes);

        const i64* rawValue = reinterpret_cast<const i64*>(&value);
        UnsafeWriteInt64(rawValue[0]);
        switch (value.Type) {
            case EValueType::Int64:
            case EValueType::Uint64:
            case EValueType::Double:
            case EValueType::Boolean:
                UnsafeWriteInt64(rawValue[1]);
                break;

            case EValueType::String:
            case EValueType::Any:
                UnsafeWriteRaw(value.Data.String, value.Length);
                break;

            default:
                break;
        }
    }

    void WriteRowValues(
        const TUnversionedValue* begin,
        const TUnversionedValue* end,
        const TNameTableToSchemaIdMapping* idMapping)
    {
        if (!begin) {
            WriteInt64(-1);
            return;
        }

        int valueCount = end - begin;
        WriteInt64(valueCount);

        if (idMapping) {
            PooledValues_.resize(valueCount);
            for (int index = 0; index < valueCount; ++index) {
                const auto& srcValue = begin[index];
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

            for (int index = 0; index < valueCount; ++index) {
                WriteRowValue(PooledValues_[index]);
            }
        } else {
            for (const auto* current = begin; current != end; ++current) {
                WriteRowValue(*current);
            }
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

TWireProtocolWriter::TWireProtocolWriter()
    : Impl_(new TImpl())
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

void TWireProtocolWriter::WriteColumnFilter(const TColumnFilter& filter)
{
    Impl_->WriteColumnFilter(filter);
}

void TWireProtocolWriter::WriteTableSchema(const TTableSchema& schema)
{
    Impl_->WriteTableSchema(schema);
}

void TWireProtocolWriter::WriteMessage(const ::google::protobuf::MessageLite& message)
{
    Impl_->WriteMessage(message);
}

void TWireProtocolWriter::WriteUnversionedRow(
    TUnversionedRow row,
    const TNameTableToSchemaIdMapping* idMapping)
{
    Impl_->WriteUnversionedRow(row, idMapping);
}

void TWireProtocolWriter::WriteUnversionedRow(
    const std::vector<TUnversionedValue>& row,
    const TNameTableToSchemaIdMapping* idMapping)
{
    Impl_->WriteUnversionedRow(row, idMapping);
}

void TWireProtocolWriter::WriteUnversionedRowset(
    const std::vector<TUnversionedRow>& rowset,
    const TNameTableToSchemaIdMapping* idMapping)
{
    Impl_->WriteUnversionedRowset(rowset, idMapping);
}

ISchemafulWriterPtr TWireProtocolWriter::CreateSchemafulRowsetWriter()
{
    return New<TSchemafulRowsetWriter>(this);
}

////////////////////////////////////////////////////////////////////////////////

class TWireProtocolReader::TSchemafulRowsetReader
    : public ISchemafulReader
{
public:
    explicit TSchemafulRowsetReader(TWireProtocolReader* reader)
        : Reader_(reader)
        , Finished_(false)
    { }

    virtual TAsyncError Open(const TTableSchema& schema) override
    {
        auto actualSchema = Reader_->ReadTableSchema();
        if (schema != actualSchema) {
            return MakeFuture(TError("Schema mismatch while parsing wire protocol"));
        }
        return OKFuture;
    }

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
            Reader_->ReadUnversionedRowset(rows);
        }
        Finished_ = true;
        return true;
    }

    virtual TAsyncError GetReadyEvent() override
    {
        return OKFuture;
    }

private:
    TWireProtocolReader* Reader_;
    bool Finished_;

};

////////////////////////////////////////////////////////////////////////////////

struct TAlignedWireProtocolReaderPoolTag { };
struct TUnalignedWireProtocolReaderPoolTag { };

class TWireProtocolReader::TImpl
{
public:
    explicit TImpl(const TSharedRef& data)
        : Data_(data)
        , Current_(Data_.Begin())
        , AlignedPool_(
        	TAlignedWireProtocolReaderPoolTag(),
        	ReaderAlignedChunkSize)
        , UnalignedPool_(
        	TUnalignedWireProtocolReaderPoolTag(),
            ReaderUnalignedChunkSize)
    {
        ProtocolVersion_ = ReadInt64();
        if (ProtocolVersion_ != CurrentProtocolVersion) {
            THROW_ERROR_EXCEPTION("Unsupported wire protocol version %" PRId64,
                ProtocolVersion_);
        }
    }

    EWireProtocolCommand ReadCommand()
    {
        return EWireProtocolCommand(ReadInt64());
    }

    TColumnFilter ReadColumnFilter()
    {
        TColumnFilter filter;
        int columnCount = ReadInt32();
        if (columnCount != -1) {
            filter.All = false;
            for (int index = 0; index < columnCount; ++index) {
                filter.Indexes.push_back(ReadInt64());
            }
            std::sort(filter.Indexes.begin(), filter.Indexes.end());
        }
        return filter;
    }

    TTableSchema ReadTableSchema()
    {
        NVersionedTableClient::NProto::TTableSchemaExt protoSchema;
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

    TUnversionedRow ReadUnversionedRow()
    {
        return ReadRow();
    }

    void ReadUnversionedRowset(std::vector<TUnversionedRow>* rowset)
    {
        int rowCount = ReadInt32();
        ValidateRowCount(rowCount);
        rowset->reserve(rowset->size() + rowCount);
        for (int index = 0; index != rowCount; ++index) {
            rowset->push_back(ReadRow());
        }
    }

private:
    TSharedRef Data_;
    const char* Current_;

    i64 ProtocolVersion_;

    TChunkedMemoryPool AlignedPool_;
    TChunkedMemoryPool UnalignedPool_;


    i64 ReadInt64()
    {
        i64 result = *reinterpret_cast<const i64*>(Current_);
        Current_ += sizeof (result);
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


    void ReadRaw(void* buffer, size_t size)
    {
        memcpy(buffer, Current_, size);
        Current_ += size;
        Current_ += GetPaddingSize(size);
    }


    Stroka ReadString()
    {
        size_t length = ReadInt64();
        Stroka value(length);
        ReadRaw(const_cast<char*>(value.data()), length);
        return value;
    }

    void ReadRowValue(TUnversionedValue* value)
    {
        i64* rawValue = reinterpret_cast<i64*>(value);
        rawValue[0] = ReadInt64();

        switch (value->Type) {
            case EValueType::Int64:
            case EValueType::Uint64:
            case EValueType::Double:
            case EValueType::Boolean:
                rawValue[1] = ReadInt64();
                break;

            case EValueType::String:
            case EValueType::Any:
                if (value->Length > MaxStringValueLength) {
                    THROW_ERROR_EXCEPTION("Value is too long: length %" PRId64 ", limit %" PRId64,
                        static_cast<i64>(value->Length),
                        MaxStringValueLength);
                }
                value->Data.String = UnalignedPool_.AllocateUnaligned(value->Length);
                ReadRaw(const_cast<char*>(value->Data.String), value->Length);
                break;

            default:
                break;
        }
    }

    TUnversionedRow ReadRow()
    {
        int valueCount = ReadInt32();
        if (valueCount == -1) {
            return TUnversionedRow();
        }

        ValidateRowValueCount(valueCount);

        auto row = TUnversionedRow::Allocate(&AlignedPool_, valueCount);
        for (int index = 0; index < valueCount; ++index) {
            ReadRowValue(&row[index]);
        }
        return row;
    }

};

////////////////////////////////////////////////////////////////////////////////

TWireProtocolReader::TWireProtocolReader(const TSharedRef& data)
    : Impl_(new TImpl(data))
{ }

TWireProtocolReader::~TWireProtocolReader()
{ }

EWireProtocolCommand TWireProtocolReader::ReadCommand()
{
    return Impl_->ReadCommand();
}

TColumnFilter TWireProtocolReader::ReadColumnFilter()
{
    return Impl_->ReadColumnFilter();
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

void TWireProtocolReader::ReadUnversionedRowset(std::vector<TUnversionedRow>* rowset)
{
    Impl_->ReadUnversionedRowset(rowset);
}

ISchemafulReaderPtr TWireProtocolReader::CreateSchemafulRowsetReader()
{
    return New<TSchemafulRowsetReader>(this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT


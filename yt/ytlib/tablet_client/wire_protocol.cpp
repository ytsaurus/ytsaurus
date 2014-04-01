#include "stdafx.h"
#include "wire_protocol.h"

#include <core/misc/error.h>
#include <core/misc/zigzag.h>
#include <core/misc/chunked_memory_pool.h>
#include <core/misc/protobuf_helpers.h>

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/schemaful_reader.h>
#include <ytlib/new_table_client/schemaful_writer.h>
#include <ytlib/new_table_client/chunk_meta.pb.h>

#include <contrib/libs/protobuf/io/coded_stream.h>
#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT {
namespace NTabletClient {

using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

static const ui32 CurrentProtocolVersion = 1;
static const size_t ReaderAlignedChunkSize = 16384;
static const size_t ReaderUnalignedChunkSize = 16384;

static const auto PresetResult = MakeFuture(TError());

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
        return PresetResult;
    }

    virtual TAsyncError Close() override
    {
        Writer_->WriteCommand(EWireProtocolCommand::EndOfRowset);
        return PresetResult;
    }

    virtual bool Write(const std::vector<TUnversionedRow>& rows) override
    {
        Writer_->WriteCommand(EWireProtocolCommand::RowsetChunk);
        Writer_->WriteUnversionedRowset(rows);
        return true;
    }

    virtual TAsyncError GetReadyEvent() override
    {
        return PresetResult;
    }

private:
    TWireProtocolWriter* Writer_;

};

////////////////////////////////////////////////////////////////////////////////

class TWireProtocolWriter::TImpl
{
public:
    TImpl()
        : RawStream_(new google::protobuf::io::StringOutputStream(&Data_))
        , CodedStream_(new google::protobuf::io::CodedOutputStream(RawStream_.get()))
    {
        WriteUInt32(CurrentProtocolVersion);
    }

    void WriteCommand(EWireProtocolCommand command)
    {
        WriteUInt32(command);
    }

    void WriteColumnFilter(const TColumnFilter& filter)
    {
        if (filter.All) {
            WriteUInt32(0);
        } else {
            WriteUInt32(filter.Indexes.size() + 1);
            for (int index : filter.Indexes) {
                WriteUInt32(index);
            }
        }
    }

    void WriteTableSchema(const TTableSchema& schema)
    {
        WriteMessage(ToProto<NVersionedTableClient::NProto::TTableSchemaExt>(schema));
    }

    void WriteMessage(const ::google::protobuf::MessageLite& message)
    {
        WriteUInt32(message.ByteSize());

        message.SerializePartialToCodedStream(CodedStream_.get());
    }

    void WriteUnversionedRow(
        TUnversionedRow row,
        const TColumnIdMapping* idMapping)
    {
        if (row) {
            WriteUInt32(row.GetCount() + 1);
            for (int index = 0; index < row.GetCount(); ++index) {
                WriteRowValue(row[index], idMapping);
            }
        } else {
            WriteUInt32(0);
        }
    }

    void WriteUnversionedRow(
        const std::vector<TUnversionedValue>& row,
        const TColumnIdMapping* idMapping)
    {
        WriteUInt32(row.size() + 1);
        for (int index = 0; index < row.size(); ++index) {
            WriteRowValue(row[index], idMapping);
        }
    }

    void WriteUnversionedRowset(
        const std::vector<TUnversionedRow>& rowset,
        const TColumnIdMapping* idMapping)
    {
        WriteUInt32(rowset.size());
        for (auto row : rowset) {
            WriteUnversionedRow(row, idMapping);
        }
    }

    Stroka GetData()
    {
        // Destroying the streams also trims the (preallocated) tail of the string.
        CodedStream_.reset();
        RawStream_.reset();
        return Data_;
    }

private:
    Stroka Data_;
    std::unique_ptr<google::protobuf::io::StringOutputStream> RawStream_;
    std::unique_ptr<google::protobuf::io::CodedOutputStream> CodedStream_;


    void WriteUInt32(ui32 value)
    {
        CodedStream_->WriteVarint32(value);
    }

    void WriteUInt64(ui64 value)
    {
        CodedStream_->WriteVarint64(value);
    }

    void WriteInt64(i64 value)
    {
        WriteUInt64(ZigZagEncode64(value));
    }

    void WriteDouble(double value)
    {
        WriteRaw(&value, sizeof (double));
    }

    void WriteString(const Stroka& value)
    {
        WriteUInt32(value.length());
        WriteRaw(value.begin(), value.length());
    }

    void WriteRaw(const void* buffer, size_t size)
    {
        CodedStream_->WriteRaw(buffer, size);
    }

    void WriteRowValue(
        const TUnversionedValue& value,
        const TColumnIdMapping* idMapping)
    {
        if (idMapping) {
            if (value.Id >= idMapping->size()) {
                THROW_ERROR_EXCEPTION("Invalid column id %d, expected in range [0, %d]",
                    static_cast<int>(value.Id),
                    static_cast<int>(idMapping->size()));
            }
            WriteUInt32((*idMapping)[value.Id]);
        } else {
            WriteUInt32(value.Id);
        }
        WriteUInt32(value.Type);
        switch (value.Type) {
            case EValueType::Integer:
                WriteInt64(value.Data.Integer);
                break;

            case EValueType::Double:
                WriteDouble(value.Data.Double);
                break;
            
            case EValueType::String:
            case EValueType::Any:
                WriteUInt32(value.Length);
                WriteRaw(value.Data.String, value.Length);
                break;

            default:
                break;
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

TWireProtocolWriter::TWireProtocolWriter()
    : Impl_(new TImpl())
{ }

TWireProtocolWriter::~TWireProtocolWriter()
{ }

Stroka TWireProtocolWriter::GetData()
{
    return Impl_->GetData();
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
    const TColumnIdMapping* idMapping)
{
    Impl_->WriteUnversionedRow(row, idMapping);
}

void TWireProtocolWriter::WriteUnversionedRow(
    const std::vector<TUnversionedValue>& row,
    const TColumnIdMapping* idMapping)
{
    Impl_->WriteUnversionedRow(row, idMapping);
}

void TWireProtocolWriter::WriteUnversionedRowset(
    const std::vector<TUnversionedRow>& rowset,
    const TColumnIdMapping* idMapping)
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
        return PresetResult;
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
        return PresetResult;
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
    explicit TImpl(const Stroka& data)
        : Data_(data)
        , CodedStream_(reinterpret_cast<const ui8*>(data.data()), data.length())
        , AlignedPool_(
        	TAlignedWireProtocolReaderPoolTag(),
        	ReaderAlignedChunkSize)
        , UnalignedPool_(
        	TUnalignedWireProtocolReaderPoolTag(),
            ReaderUnalignedChunkSize)
    {
        ProtocolVersion_ = ReadUInt32();
        if (ProtocolVersion_ != 1) {
            THROW_ERROR_EXCEPTION("Unsupported wire protocol version %d",
                ProtocolVersion_);
        }
    }

    EWireProtocolCommand ReadCommand()
    {
        return EWireProtocolCommand(ReadUInt32());
    }

    TColumnFilter ReadColumnFilter()
    {
        TColumnFilter filter;
        ui32 count = ReadUInt32();
        if (count != 0) {
            filter.All = false;
            for (int index = 0; index < count - 1; ++index) {
                filter.Indexes.push_back(ReadUInt32());
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
        ui32 messageSize = ReadUInt32();

        const void* chunk;
        intptr_t chunkSize;
        CheckResult(CodedStream_.GetDirectBufferPointer(&chunk, &chunkSize));
        CheckResult(chunkSize >= messageSize);

        ::google::protobuf::io::CodedInputStream chunkStream(
            reinterpret_cast<const ui8*>(chunk),
            messageSize);
        message->ParsePartialFromCodedStream(&chunkStream);

        CodedStream_.Skip(messageSize);
    }

    TUnversionedRow ReadUnversionedRow()
    {
        return ReadRow();
    }

    void ReadUnversionedRowset(std::vector<TUnversionedRow>* rowset)
    {
        ui32 count = ReadUInt32();
        rowset->reserve(rowset->size() + count);
        for (int index = 0; index != count; ++index) {
            rowset->push_back(ReadRow());
        }
    }

private:
    Stroka Data_;

    google::protobuf::io::CodedInputStream CodedStream_;

    int ProtocolVersion_;

    TChunkedMemoryPool AlignedPool_;
    TChunkedMemoryPool UnalignedPool_;


    void CheckResult(bool result)
    {
        if (!result) {
            THROW_ERROR_EXCEPTION("Error parsing wire protocol");
        }
    }

    ui32 ReadUInt32()
    {
        ui32 value;
        CheckResult(CodedStream_.ReadVarint32(&value));
        return value;
    }

    ui64 ReadUInt64()
    {
        ui64 value;
        CheckResult(CodedStream_.ReadVarint64(&value));
        return value;
    }

    i64 ReadInt64()
    {
        return ZigZagDecode64(ReadUInt64());
    }

    double ReadDouble()
    {
        double value;
        ReadRaw(&value, sizeof (value));
        return value;
    }

    Stroka ReadString()
    {
        size_t length = ReadUInt32();
        Stroka value(length);
        ReadRaw(const_cast<char*>(value.data()), length);
        return value;
    }

    void ReadRaw(void* buffer, size_t size)
    {
        CheckResult(CodedStream_.ReadRaw(buffer, size));
    }


    void ReadRowValue(TUnversionedValue* value)
    {
        value->Id = ReadUInt32();
        value->Type = ReadUInt32();
        switch (value->Type) {
            case EValueType::Integer:
                value->Data.Integer = ReadInt64();
                break;

            case EValueType::Double:
                value->Data.Double = ReadDouble();
                break;
            
            case EValueType::String:
            case EValueType::Any:
                value->Length = ReadUInt32();
                value->Data.String = UnalignedPool_.AllocateUnaligned(value->Length);
                ReadRaw(const_cast<char*>(value->Data.String), value->Length);
                break;

            default:
                break;
        }
    }

    TUnversionedRow ReadRow()
    {
        ui32 valueCount = ReadUInt32();
        if (valueCount == 0) {
            return TUnversionedRow();
        }
        --valueCount;

        auto row = TUnversionedRow::Allocate(&AlignedPool_, valueCount);
        for (int index = 0; index != valueCount; ++index) {
            ReadRowValue(&row[index]);
        }
        return row;
    }

};

////////////////////////////////////////////////////////////////////////////////

TWireProtocolReader::TWireProtocolReader(const Stroka& data)
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


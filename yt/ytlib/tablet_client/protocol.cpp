#include "stdafx.h"
#include "protocol.h"

#include <core/misc/error.h>
#include <core/misc/zigzag.h>
#include <core/misc/chunked_memory_pool.h>

#include <ytlib/new_table_client/row.h>

#include <contrib/libs/protobuf/io/coded_stream.h>
#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT {
namespace NTabletClient {

using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

static const ui32 ProtocolVersion = 1;
static const size_t ReaderAlignedChunkSize = 16384;
static const size_t ReaderUnalignedChunkSize = 16384;

////////////////////////////////////////////////////////////////////////////////

class TProtocolWriter::TImpl
{
public:
    TImpl()
        : RawStream_(&Data_)
        , CodedStream_(&RawStream_)
    {
        WriteUInt32(ProtocolVersion);
    }


    void WriteCommand(EProtocolCommand command)
    {
        WriteUInt32(command);
    }


    void WriteUnversionedRow(TUnversionedRow row)
    {
        WriteRow(row);
    }

    void WriteVersionedRow(TVersionedRow row)
    {
        WriteRow(row);
    }


    void WriteUnversionedRowset(const std::vector<TUnversionedRow>& rowset)
    {
        WriteRowset(rowset);
    }

    void WriteVersionedRowset(const std::vector<TVersionedRow>& rowset)
    {
        WriteRowset(rowset);
    }


    Stroka Finish()
    {
        WriteCommand(EProtocolCommand::End);
        return Data_;
    }

private:
    Stroka Data_;
    google::protobuf::io::StringOutputStream RawStream_;
    google::protobuf::io::CodedOutputStream CodedStream_;


    void WriteUInt32(ui32 value)
    {
        CodedStream_.WriteVarint32(value);
    }

    void WriteUInt64(ui64 value)
    {
        CodedStream_.WriteVarint64(value);
    }

    void WriteInt64(i64 value)
    {
        WriteUInt64(ZigZagEncode64(value));
    }

    void WriteDouble(double value)
    {
        WriteRaw(&value, sizeof (double));
    }

    void WriteRaw(const void* buffer, size_t size)
    {
        CodedStream_.WriteRaw(buffer, size);
    }


    void WriteRowValue(const TUnversionedValue& value)
    {
        WriteUInt32(value.Id);
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
                YUNREACHABLE();
        }
    }

    void WriteRowValue(const TVersionedValue& value)
    {
        WriteUInt64(value.Timestamp);
        WriteRowValue(static_cast<const TUnversionedValue&>(value));
    }

    template <class TValue>
    void WriteRow(TRow<TValue> row)
    {
        WriteUInt32(row.GetValueCount());
        for (int index = 0; index < row.GetValueCount(); ++index) {
            WriteRowValue(row[index]);
        }
    }

    template <class TValue>
    void WriteRowset(const std::vector<TRow<TValue>>& rowset)
    {
        WriteUInt32(rowset.size());
        for (auto row : rowset) {
            WriteRow(row);
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

TProtocolWriter::TProtocolWriter()
    : Impl_(new TImpl())
{ }

Stroka TProtocolWriter::Finish()
{
    return Impl_->Finish();
}

void TProtocolWriter::WriteCommand(EProtocolCommand command)
{
    Impl_->WriteCommand(command);
}

void TProtocolWriter::WriteUnversionedRow(TUnversionedRow row)
{
    Impl_->WriteUnversionedRow(row);
}

void TProtocolWriter::WriteVersionedRow(TVersionedRow row)
{
    Impl_->WriteVersionedRow(row);
}

void TProtocolWriter::WriteUnversionedRowset(const std::vector<TUnversionedRow>& rowset)
{
    Impl_->WriteUnversionedRowset(rowset);
}

void TProtocolWriter::WriteVersionedRowset(const std::vector<TVersionedRow>& rowset)
{
    Impl_->WriteVersionedRowset(rowset);
}

////////////////////////////////////////////////////////////////////////////////

class TProtocolReader::TImpl
{
public:
    explicit TImpl(TRef data)
        : RawStream_(data.Begin(), data.Size())
        , CodedStream_(&RawStream_)
        , AlignedPool_(ReaderAlignedChunkSize)
        , UnalignedPool_(ReaderUnalignedChunkSize)
    {
        int version = ReadUInt32();
        if (version != ProtocolVersion) {
            THROW_ERROR_EXCEPTION("Protocol version mismatch: expected %u, got %u",
                ProtocolVersion,
                version);
        }
    }


    EProtocolCommand ReadCommand()
    {
        return EProtocolCommand(ReadUInt32());
    }


    TUnversionedRow ReadUnversionedRow()
    {
        return ReadRow<TUnversionedValue>();
    }

    TVersionedRow ReadVersionedRow()
    {
        return ReadRow<TVersionedValue>();
    }


    void ReadUnversionedRowset(std::vector<TUnversionedRow>* rowset)
    {
        ReadRowset(rowset);
    }

    void ReadVersionedRowset(std::vector<TVersionedRow>* rowset)
    {
        ReadRowset(rowset);
    }

private:
    google::protobuf::io::ArrayInputStream RawStream_;
    google::protobuf::io::CodedInputStream CodedStream_;

    TChunkedMemoryPool AlignedPool_;
    TChunkedMemoryPool UnalignedPool_;


    void CheckResult(bool result)
    {
        if (!result) {
            THROW_ERROR_EXCEPTION("Error parsing protocol");
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
                YUNREACHABLE();
        }
    }

    void ReadRowValue(TVersionedValue* value)
    {
        value->Timestamp = ReadUInt64();
        ReadRowValue(static_cast<TUnversionedValue*>(value));
    }

    template <class TValue>
    TRow<TValue> ReadRow()
    {
        ui32 count = ReadUInt32();
        auto* header = reinterpret_cast<TRowHeader*>(AlignedPool_.Allocate(GetRowDataSize<TValue>(count)));
        header->ValueCount = count;
        auto row = TRow<TValue>(header);
        for (int index = 0; index != count; ++index) {
            ReadRowValue(&row[index]);
        }
        return row;
    }

    template <class TValue>
    void ReadRowset(std::vector<TRow<TValue>>* rowset)
    {
        ui32 count = ReadUInt32();
        rowset->resize(count);
        for (int index = 0; index != count; ++index) {
            (*rowset)[index] = ReadRow<TValue>();
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

TProtocolReader::TProtocolReader(TRef data)
    : Impl_(new TImpl(data))
{ }

EProtocolCommand TProtocolReader::ReadCommand()
{
    return Impl_->ReadCommand();
}

TUnversionedRow TProtocolReader::ReadUnversionedRow()
{
    return Impl_->ReadUnversionedRow();
}

void TProtocolReader::ReadUnversionedRowset(std::vector<TUnversionedRow>* rowset)
{
    Impl_->ReadUnversionedRowset(rowset);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT


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

static const ui32 CurrentProtocolVersion = 1;
static const size_t ReaderAlignedChunkSize = 16384;
static const size_t ReaderUnalignedChunkSize = 16384;

////////////////////////////////////////////////////////////////////////////////

TColumnFilter::TColumnFilter()
    : All(true)
{ }

TColumnFilter::TColumnFilter(const std::vector<Stroka>& columns)
    : All(false)
    , Columns(columns.begin(), columns.end())
{ }

TColumnFilter::TColumnFilter(const TColumnFilter& other)
    : All(other.All)
    , Columns(other.Columns)
{ }

////////////////////////////////////////////////////////////////////////////////

class TProtocolWriter::TImpl
{
public:
    TImpl()
        : RawStream_(&Data_)
        , CodedStream_(&RawStream_)
    {
        WriteUInt32(CurrentProtocolVersion);
    }


    void WriteCommand(EProtocolCommand command)
    {
        WriteUInt32(command);
    }


    void WriteColumnFilter(const TColumnFilter& filter)
    {
        WriteUInt32(filter.All);
        if (!filter.All) {
            WriteUInt32(filter.Columns.size());
            for (const auto& name : filter.Columns) {
                WriteString(name);
            }
        }
    }


    void WriteUnversionedRow(TUnversionedRow row)
    {
        WriteRow(row);
    }

    void WriteUnversionedRow(const std::vector<TUnversionedValue>& row)
    {
        WriteRow(row);
    }
    
    void WriteVersionedRow(TVersionedRow row)
    {
        WriteRow(row);
    }

    void WriteVersionedRow(const std::vector<TVersionedValue>& row)
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

    void WriteString(const Stroka& value)
    {
        WriteUInt32(value.length());
        WriteRaw(value.begin(), value.length());
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
                break;
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
    void WriteRow(const std::vector<TValue>& row)
    {
        WriteUInt32(row.size());
        for (int index = 0; index < static_cast<int>(row.size()); ++index) {
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

TProtocolWriter::~TProtocolWriter()
{ }

Stroka TProtocolWriter::Finish()
{
    return Impl_->Finish();
}

void TProtocolWriter::WriteCommand(EProtocolCommand command)
{
    Impl_->WriteCommand(command);
}

void TProtocolWriter::WriteColumnFilter(const TColumnFilter& filter)
{
    Impl_->WriteColumnFilter(filter);
}

void TProtocolWriter::WriteUnversionedRow(TUnversionedRow row)
{
    Impl_->WriteUnversionedRow(row);
}

void TProtocolWriter::WriteUnversionedRow(const std::vector<TUnversionedValue>& row)
{
    Impl_->WriteUnversionedRow(row);
}

void TProtocolWriter::WriteVersionedRow(TVersionedRow row)
{
    Impl_->WriteVersionedRow(row);
}

void TProtocolWriter::WriteVersionedRow(const std::vector<TVersionedValue>& row)
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
    explicit TImpl(const Stroka& data)
        : RawStream_(data.data(), data.length())
        , CodedStream_(&RawStream_)
        , AlignedPool_(ReaderAlignedChunkSize)
        , UnalignedPool_(ReaderUnalignedChunkSize)
    {
        ProtocolVersion_ = ReadUInt32();
        if (ProtocolVersion_ != 1) {
            THROW_ERROR_EXCEPTION("Unsupported protocol version %d",
                ProtocolVersion_);
        }
    }


    EProtocolCommand ReadCommand()
    {
        return EProtocolCommand(ReadUInt32());
    }

    
    TColumnFilter ReadColumnFilter()
    {
        TColumnFilter filter;
        filter.All = ReadUInt32();
        if (!filter.All) {
            ui32 count = ReadUInt32();
            for (int index = 0; index < count; ++index) {
                filter.Columns.push_back(ReadString());
            }
        }
        return filter;
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

    int ProtocolVersion_;

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

TProtocolReader::TProtocolReader(const Stroka& data)
    : Impl_(new TImpl(data))
{ }

TProtocolReader::~TProtocolReader()
{ }

EProtocolCommand TProtocolReader::ReadCommand()
{
    return Impl_->ReadCommand();
}

TColumnFilter TProtocolReader::ReadColumnFilter()
{
    return Impl_->ReadColumnFilter();
}

TUnversionedRow TProtocolReader::ReadUnversionedRow()
{
    return Impl_->ReadUnversionedRow();
}

TVersionedRow TProtocolReader::ReadVersionedRow()
{
    return Impl_->ReadVersionedRow();
}

void TProtocolReader::ReadUnversionedRowset(std::vector<TUnversionedRow>* rowset)
{
    Impl_->ReadUnversionedRowset(rowset);
}

void TProtocolReader::ReadVersionedRowset(std::vector<TVersionedRow>* rowset)
{
    Impl_->ReadVersionedRowset(rowset);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT


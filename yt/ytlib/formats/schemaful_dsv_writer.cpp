#include "stdafx.h"
#include "schemaful_dsv_writer.h"

#include <ytlib/table_client/name_table.h>

#include <core/misc/error.h>

#include <core/yson/format.h>

namespace NYT {
namespace NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TSchemafulDsvWriterBase::TSchemafulWriterForSchemafulDsvBase(TSchemafulDsvFormatConfigPtr config)
    : Config_(config)
{ }

static ui16 DigitPairs[100] = {
    12336,  12337,  12338,  12339,  12340,  12341,  12342,  12343,  12344,  12345,
    12592,  12593,  12594,  12595,  12596,  12597,  12598,  12599,  12600,  12601,
    12848,  12849,  12850,  12851,  12852,  12853,  12854,  12855,  12856,  12857,
    13104,  13105,  13106,  13107,  13108,  13109,  13110,  13111,  13112,  13113,
    13360,  13361,  13362,  13363,  13364,  13365,  13366,  13367,  13368,  13369,
    13616,  13617,  13618,  13619,  13620,  13621,  13622,  13623,  13624,  13625,
    13872,  13873,  13874,  13875,  13876,  13877,  13878,  13879,  13880,  13881,
    14128,  14129,  14130,  14131,  14132,  14133,  14134,  14135,  14136,  14137,
    14384,  14385,  14386,  14387,  14388,  14389,  14390,  14391,  14392,  14393,
    14640,  14641,  14642,  14643,  14644,  14645,  14646,  14647,  14648,  14649
};

char* TSchemafulDsvWriterBase::WriteInt64Reversed(char* ptr, i64 value)
{
    if (value == 0) {
        *ptr++ = '0';
        return ptr;
    }

    bool negative = false;
    if (value < 0) {
        negative = true;
        value = -value;
    }

    while (value >= 10) {
        i64 rem = value % 100;
        i64 quot = value / 100;
        *reinterpret_cast<ui16*>(ptr) = DigitPairs[rem];
        ptr += 2;
        value = quot;
    }

    if (value > 0) {
        *ptr++ = ('0' + value);
    }

    if (negative) {
        *ptr++ = '-';
    }

    return ptr;
}

char* TSchemafulDsvWriterBase::WriteUint64Reversed(char* ptr, ui64 value)
{
    if (value == 0) {
        *ptr++ = '0';
        return ptr;
    }

    while (value >= 10) {
        i64 rem = value % 100;
        i64 quot = value / 100;
        *reinterpret_cast<ui16*>(ptr) = DigitPairs[rem];
        ptr += 2;
        value = quot;
    }

    if (value > 0) {
        *ptr++ = ('0' + value);
    }

    return ptr;
}

void TSchemafulDsvWriterBase::WriteValue(const TUnversionedValue& value)
{
    switch (value.Type) {
        case EValueType::Null:
            break;

        case EValueType::Int64:
        case EValueType::Uint64: {
            char buf[64];
            char* begin = buf;
            char* end = value.Type == EValueType::Int64
                ? WriteInt64Reversed(begin, value.Data.Int64)
                : WriteUint64Reversed(begin, value.Data.Uint64);
            size_t length = end - begin;

            Buffer_.Resize(Buffer_.Size() + length, false);
            char* src = begin;
            char* dst = Buffer_.End() - 1;
            while (src != end) {
                *dst-- = *src++;
            }
            break;
        }

        case EValueType::Double: {
            // TODO(babenko): optimize
            const size_t maxSize = 64;
            Buffer_.Resize(Buffer_.Size() + maxSize);
            int size = sprintf(Buffer_.End() - maxSize, "%lg", value.Data.Double);
            Buffer_.Resize(Buffer_.Size() - maxSize + size);
            break;
        }

        case EValueType::Boolean: {
            WriteRaw(FormatBool(value.Data.Boolean));
            break;
        }

        case EValueType::String:
            WriteRaw(TStringBuf(value.Data.String, value.Length));
            break;

        default:
            WriteRaw('?');
            break;
    }
}

void TSchemafulDsvWriterBase::WriteRaw(const TStringBuf& str)
{
    Buffer_.Append(str.begin(), str.length());
}

void TSchemafulDsvWriterBase::WriteRaw(char ch)
{
    Buffer_.Append(ch);
}

////////////////////////////////////////////////////////////////////////////////

TSchemalessWriterForSchemafulDsv::TSchemalessWriterForSchemafulDsv(
    TNameTablePtr nameTable, 
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TSchemafulDsvFormatConfigPtr config)
    : TSchemalessFormatWriterBase(
        nameTable,
        std::move(output),
        enableContextSaving,
        false /* enableKeySwitch */,
        0 /* keyColumnCount */)
    , TSchemafulDsvWriterBase(config)
{
    YCHECK(Config_->Columns);
    const auto& columns = Config_->Columns.Get();
    for (int columnIndex = 0; columnIndex < static_cast<int>(columns.size()); ++columnIndex) {
        ColumnIdMapping_.push_back(NameTable_->GetId(columns[columnIndex]));
    }
    IdToIndexInRowMapping_.resize(nameTable->GetSize());
}

void TSchemalessWriterForSchemafulDsv::DoWrite(const std::vector<NTableClient::TUnversionedRow>& rows)
{
    for (auto row : rows) {
        IdToIndexInRowMapping_.assign(IdToIndexInRowMapping_.size(), -1);        
        for (auto item = row.Begin(); item != row.End(); ++item) {
            IdToIndexInRowMapping_[item->Id] = item - row.Begin();
        }
        bool firstValue = true;
        for (auto idMappingCurrent : ColumnIdMapping_) {
            if (!firstValue) {
                WriteRaw(Config_->FieldSeparator);
            } else {
                firstValue = false;
            }
            int index = IdToIndexInRowMapping_[idMappingCurrent];
            if (index == -1) {
                THROW_ERROR_EXCEPTION("Column %Qv is in schema but missing", NameTable_->GetName(idMappingCurrent));
            }
            WriteValue(row[index]);
        }
        WriteRaw(Config_->RecordSeparator);
    }
    
    auto* stream = GetOutputStream();
    stream->Write(TStringBuf(Buffer_.Begin(), Buffer_.Size()));
}

void TSchemalessWriterForSchemafulDsv::WriteTableIndex(i32 tableIndex)
{
    THROW_ERROR_EXCEPTION("Table inidices are not supported in schemaful DSV");
}

void TSchemalessWriterForSchemafulDsv::WriteRangeIndex(i32 rangeIndex)
{
    THROW_ERROR_EXCEPTION("Range inidices are not supported in schemaful DSV");
}
    
void TSchemalessWriterForSchemafulDsv::WriteRowIndex(i64 rowIndex)
{
    THROW_ERROR_EXCEPTION("Row inidices are not supported in schemaful DSV");
}

////////////////////////////////////////////////////////////////////////////////

TSchemafulWriterForSchemafulDsv::TSchemafulWriterForSchemafulDsv(
    IAsyncOutputStreamPtr stream,
    std::vector<int> columnIdMapping,
    TSchemafulDsvFormatConfigPtr config)
    : TSchemafulDsvWriterBase(config)
    , Stream_(stream)
{
   ColumnIdMapping_.swap(columnIdMapping); 
}

TFuture<void> TSchemafulWriterForSchemafulDsv::Close()
{
    return VoidFuture;
}

bool TSchemafulWriterForSchemafulDsv::Write(const std::vector<TUnversionedRow>& rows)
{
    Buffer_.Clear();

    for (auto row : rows) {
        bool firstValue = true;
        for (auto id : ColumnIdMapping_) {
            if (!firstValue) {
                WriteRaw(Config_->FieldSeparator);
            } else {
                firstValue = false;
            }
            WriteValue(row[id]);
        }
        WriteRaw(Config_->RecordSeparator);
    }
    
    auto buffer = TSharedRef::FromBlob(std::move(Buffer_));
    Result_ = Stream_->Write(buffer);
    return Result_.IsSet() && Result_.Get().IsOK();
}

TFuture<void> TSchemafulWriterForSchemafulDsv::GetReadyEvent()
{
    return Result_;
}

////////////////////////////////////////////////////////////////////////////////

NTableClient::ISchemafulWriterPtr CreateSchemafulWriterForSchemafulDsv(
    NConcurrency::IAsyncOutputStreamPtr stream,
    const NTableClient::TTableSchema& schema,
    TSchemafulDsvFormatConfigPtr config)
{
    std::vector<int> columnIdMapping;
    if (config->Columns) {
        for (const auto& name : *config->Columns) {
            columnIdMapping.push_back(schema.GetColumnIndexOrThrow(name));
        }
    } else {
        for (int id = 0; id < schema.Columns().size(); ++id) {
            columnIdMapping.push_back(id);
        }
    }

    return New<TSchemafulWriterForSchemafulDsv>(stream, std::move(columnIdMapping), config);
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NFormats
} // namespace NYT

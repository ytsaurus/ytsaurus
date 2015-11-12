#include "stdafx.h"
#include "schemaful_dsv_writer.h"

#include <ytlib/table_client/name_table.h>

#include <core/misc/error.h>

#include <core/yson/format.h>

#include <limits>

namespace NYT {
namespace NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TSchemafulDsvWriterBase::TSchemafulDsvWriterBase(
        TSchemafulDsvFormatConfigPtr config)
    : Config_(config)
    , Table_(config)
{ }

static ui16 DigitPairs[100] = {
    12336,  12592,  12848,  13104,  13360,  13616,  13872,  14128,  14384,  14640,
    12337,  12593,  12849,  13105,  13361,  13617,  13873,  14129,  14385,  14641,
    12338,  12594,  12850,  13106,  13362,  13618,  13874,  14130,  14386,  14642,
    12339,  12595,  12851,  13107,  13363,  13619,  13875,  14131,  14387,  14643,
    12340,  12596,  12852,  13108,  13364,  13620,  13876,  14132,  14388,  14644,
    12341,  12597,  12853,  13109,  13365,  13621,  13877,  14133,  14389,  14645,
    12342,  12598,  12854,  13110,  13366,  13622,  13878,  14134,  14390,  14646,
    12343,  12599,  12855,  13111,  13367,  13623,  13879,  14135,  14391,  14647,
    12344,  12600,  12856,  13112,  13368,  13624,  13880,  14136,  14392,  14648,
    12345,  12601,  12857,  13113,  13369,  13625,  13881,  14137,  14393,  14649,
};

// This function fills a specific range in the memory with a decimal representation
// of value in backwards, meaning that the resulting representation will occupy
// range [ptr - length, ptr). Return value is ptr - length, i. e. the pointer to the
// beginning of the result.
char* TSchemafulDsvWriterBase::WriteInt64Backwards(char* ptr, i64 value)
{
    if (value == 0) {
        --ptr;
        *ptr = '0';
        return ptr;
    }

    // The negative value handling code below works incorrectly for value = -2^63.
    if (value == std::numeric_limits<i64>::min()) {
        ptr -= 20;
        memcpy(ptr, "-9223372036854775808", 20);
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
        ptr -= 2;
        *reinterpret_cast<ui16*>(ptr) = DigitPairs[rem];
        value = quot;
    }

    if (value > 0) {
        --ptr;
        *ptr = ('0' + value);
    }

    if (negative) {
        --ptr;
        *ptr = '-';
    }

    return ptr;
}

// Same as WriteInt64Backwards for ui64.
char* TSchemafulDsvWriterBase::WriteUint64Backwards(char* ptr, ui64 value)
{
    if (value == 0) {
        --ptr;
        *ptr = '0';
        return ptr;
    }

    while (value >= 10) {
        i64 rem = value % 100;
        i64 quot = value / 100;
        ptr -= 2;
        *reinterpret_cast<ui16*>(ptr) = DigitPairs[rem];
        value = quot;
    }

    if (value > 0) {
        --ptr;
        *ptr = ('0' + value);
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
            char* end = buf + 64;
            char* begin = value.Type == EValueType::Int64
                ? WriteInt64Backwards(end, value.Data.Int64)
                : WriteUint64Backwards(end, value.Data.Uint64);
            size_t length = end - begin;
            BlobOutput_->Write(begin, length);
            break;
        }

        case EValueType::Double: {
            // TODO(babenko): optimize
            char buf[64];
            char* begin = buf;
            int length = sprintf(buf, "%lg", value.Data.Double);
            BlobOutput_->Write(begin, length);
            break;
        }

        case EValueType::Boolean: {
            WriteRaw(FormatBool(value.Data.Boolean));
            break;
        }

        case EValueType::String: {
            EscapeAndWrite(TStringBuf(value.Data.String, value.Length));
            break;
        }

        default: {
            WriteRaw('?');
            break;
        }
    }
}

void TSchemafulDsvWriterBase::WriteRaw(const TStringBuf& str)
{
    BlobOutput_->Write(str.begin(), str.length());
}

void TSchemafulDsvWriterBase::WriteRaw(char ch)
{
    BlobOutput_->Write(ch);
}

void TSchemafulDsvWriterBase::EscapeAndWrite(const TStringBuf& string)
{
    if (Config_->EnableEscaping) {
        WriteEscaped(
            BlobOutput_,
            string,
            Table_.Stops,
            Table_.Escapes,
            Config_->EscapingSymbol);
    } else {
        BlobOutput_->Write(string);
    }
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
    BlobOutput_ = GetOutputStream();
    YCHECK(Config_->Columns);
    const auto& columns = Config_->Columns.Get();
    for (int columnIndex = 0; columnIndex < static_cast<int>(columns.size()); ++columnIndex) {
        ColumnIdMapping_.push_back(NameTable_->GetIdOrRegisterName(columns[columnIndex]));
    }
    IdToIndexInRowMapping_.resize(nameTable->GetSize());
}

void TSchemalessWriterForSchemafulDsv::DoWrite(const std::vector<TUnversionedRow>& rows)
{
    for (auto row : rows) {
        IdToIndexInRowMapping_.assign(IdToIndexInRowMapping_.size(), -1);
        for (auto item = row.Begin(); item != row.End(); ++item) {
            IdToIndexInRowMapping_[item->Id] = item - row.Begin();
        }
        bool firstValue = true;
        for (auto currentId : ColumnIdMapping_) {
            if (!firstValue) {
                WriteRaw(Config_->FieldSeparator);
            } else {
                firstValue = false;
            }
            int index = IdToIndexInRowMapping_[currentId];
            if (index == -1) {
                THROW_ERROR_EXCEPTION("Column %Qv is in schema but missing", NameTable_->GetName(currentId));
            }
            WriteValue(row[index]);
        }
        WriteRaw(Config_->RecordSeparator);
        TryFlushBuffer(false);
    }    
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
    , Output_(CreateSyncAdapter(stream))
{
    BlobOutput_ = &UnderlyingBlobOutput_; 
    ColumnIdMapping_.swap(columnIdMapping); 
}

TFuture<void> TSchemafulWriterForSchemafulDsv::Close()
{
    DoFlushBuffer();
    return VoidFuture;
}

bool TSchemafulWriterForSchemafulDsv::Write(const std::vector<TUnversionedRow>& rows)
{
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
        TryFlushBuffer(false);
    }
    
    return true;
}

// TODO(max42): Eliminate copy-paste from schemaless_writer_adapter.cpp.
void TSchemafulWriterForSchemafulDsv::TryFlushBuffer(bool force)
{
    if (force || UnderlyingBlobOutput_.Size() >= UnderlyingBlobOutput_.Blob().Capacity() / 2) {
        DoFlushBuffer();
    }
}

void TSchemafulWriterForSchemafulDsv::DoFlushBuffer()
{
    if (UnderlyingBlobOutput_.Size() == 0) {
        return;
    }

    const auto& buffer = UnderlyingBlobOutput_.Blob();
    Output_->Write(buffer.Begin(), buffer.Size());

    UnderlyingBlobOutput_.Clear();
}

TFuture<void> TSchemafulWriterForSchemafulDsv::GetReadyEvent()
{
    return Result_;
}

////////////////////////////////////////////////////////////////////////////////

ISchemafulWriterPtr CreateSchemafulWriterForSchemafulDsv(
    IAsyncOutputStreamPtr stream,
    const TTableSchema& schema,
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

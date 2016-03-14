#include "schemaful_dsv_writer.h"

#include <yt/ytlib/table_client/name_table.h>

#include <yt/core/misc/common.h>
#include <yt/core/misc/error.h>

#include <yt/core/yson/format.h>

#include <limits>

namespace NYT {
namespace NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TSchemafulDsvWriterBase::TSchemafulDsvWriterBase(
        TSchemafulDsvFormatConfigPtr config, std::vector<int> idToIndexInRow)
    : Config_(config)
    , IdToIndexInRow_(idToIndexInRow)
    , Table_(config)
{
    CurrentRowValues_.resize(
        *std::max_element(IdToIndexInRow_.begin(), IdToIndexInRow_.end()) + 1);
    YCHECK(Config_->Columns); 
}

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
char* TSchemafulDsvWriterBase::WriteInt64ToBufferBackwards(char* ptr, i64 value)
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
char* TSchemafulDsvWriterBase::WriteUint64ToBufferBackwards(char* ptr, ui64 value)
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

int TSchemafulDsvWriterBase::FindMissingValueIndex() const 
{
    for (int valueIndex = 0; valueIndex < static_cast<int>(CurrentRowValues_.size()); ++valueIndex) {
        const auto* value = CurrentRowValues_[valueIndex];
        if (!value || value->Type == EValueType::Null) {
            return valueIndex;
        }
    }
    return -1;
}

void TSchemafulDsvWriterBase::WriteValue(const TUnversionedValue& value)
{
    switch (value.Type) {
        case EValueType::Null:
            break;

        case EValueType::Int64: {
            WriteInt64(value.Data.Int64);
            break;
        }

        case EValueType::Uint64: {
            WriteUint64(value.Data.Uint64);
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

void TSchemafulDsvWriterBase::WriteInt64(i64 value) 
{
    char buf[64];
    char* end = buf + 64;
    char* start = WriteInt64ToBufferBackwards(end, value);
    BlobOutput_->Write(start, end - start);
}

void TSchemafulDsvWriterBase::WriteUint64(ui64 value) 
{
    char buf[64];
    char* end = buf + 64;
    char* start = WriteUint64ToBufferBackwards(end, value);
    BlobOutput_->Write(start, end - start);
}

////////////////////////////////////////////////////////////////////////////////

TSchemalessWriterForSchemafulDsv::TSchemalessWriterForSchemafulDsv(
    TNameTablePtr nameTable, 
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TSchemafulDsvFormatConfigPtr config,
    std::vector<int> idToIndexInRow)
    : TSchemalessFormatWriterBase(
        nameTable,
        std::move(output),
        enableContextSaving,
        false /* enableKeySwitch */,
        0 /* keyColumnCount */)
    , TSchemafulDsvWriterBase(
        config,
        idToIndexInRow)
{
    BlobOutput_ = GetOutputStream();
}

void TSchemalessWriterForSchemafulDsv::DoWrite(const std::vector<TUnversionedRow>& rows)
{
    for (const auto& row : rows) {
        CurrentRowValues_.assign(CurrentRowValues_.size(), nullptr);
        for (auto item = row.Begin(); item != row.End(); ++item) {
            if (item->Id < IdToIndexInRow_.size() && IdToIndexInRow_[item->Id] != -1) {
                CurrentRowValues_[IdToIndexInRow_[item->Id]] = item;
            }
        }
        
        int missingValueIndex = FindMissingValueIndex();
        if (missingValueIndex != -1) {
            if (Config_->MissingValueMode == EMissingSchemafulDsvValueMode::SkipRow) {
                continue;
            } else if (Config_->MissingValueMode == EMissingSchemafulDsvValueMode::Fail) { 
                THROW_ERROR_EXCEPTION("Column %Qv is in schema but missing", (*Config_->Columns)[missingValueIndex]);
            }
        }

        bool firstValue = true;
        if (Config_->EnableTableIndex) {
            if (CurrentTableIndex_ == -1) {
                THROW_ERROR_EXCEPTION("When enable_table_index = true, table index should be set before writing rows");
            }
            WriteInt64(CurrentTableIndex_);
            firstValue = false;
        }

        for (const auto* item : CurrentRowValues_) {
            if (!firstValue) {
                WriteRaw(Config_->FieldSeparator);
            } else {
                firstValue = false;
            }
            if (!item || item->Type == EValueType::Null) {
                // If we got here, MissingValueMode is PrintSentinel.
                WriteRaw(Config_->MissingValueSentinel);
            } else {
                WriteValue(*item);
            }
        }
        WriteRaw(Config_->RecordSeparator);
        TryFlushBuffer(false);
    }    
    TryFlushBuffer(true);
}

void TSchemalessWriterForSchemafulDsv::WriteTableIndex(i32 tableIndex)
{
    CurrentTableIndex_ = tableIndex;
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
    TSchemafulDsvFormatConfigPtr config,
    std::vector<int> idToIndexInRow)
    : TSchemafulDsvWriterBase(
        config,
        idToIndexInRow)
    , Output_(CreateSyncAdapter(stream))
{
    BlobOutput_ = &UnderlyingBlobOutput_; 
}

TFuture<void> TSchemafulWriterForSchemafulDsv::Close()
{
    DoFlushBuffer();
    return VoidFuture;
}

bool TSchemafulWriterForSchemafulDsv::Write(const std::vector<TUnversionedRow>& rows)
{
    for (const auto& row : rows) {
        CurrentRowValues_.assign(CurrentRowValues_.size(), nullptr);
        for (auto item = row.Begin(); item != row.End(); ++item) {
            YASSERT(item->Id >= 0 && item->Id < IdToIndexInRow_.size());
            if (IdToIndexInRow_[item->Id] != -1) {
                CurrentRowValues_[IdToIndexInRow_[item->Id]] = item;
            }
        }
        
        int missingValueIndex = FindMissingValueIndex();
        if (missingValueIndex != -1) {
            if (Config_->MissingValueMode == EMissingSchemafulDsvValueMode::SkipRow) {
                continue;
            } else if (Config_->MissingValueMode == EMissingSchemafulDsvValueMode::Fail) { 
                THROW_ERROR_EXCEPTION("Column %Qv is in schema but missing", (*Config_->Columns)[missingValueIndex]);
            }
        }

        bool firstValue = true;
        for (const auto* item : CurrentRowValues_) {
            if (!firstValue) {
                WriteRaw(Config_->FieldSeparator);
            } else {
                firstValue = false;
            }
            if (!item || item->Type == EValueType::Null) {
                // If we got here, MissingValueMode is PrintSentinel.
                WriteRaw(Config_->MissingValueSentinel);
            } else {
                WriteValue(*item);
            }
        }
        WriteRaw(Config_->RecordSeparator);
        TryFlushBuffer(false);
    }    
    TryFlushBuffer(true);
    
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
    std::vector<int> idToIndexInRow(schema.Columns().size(), -1);
    if (config->Columns) {
        for (int columnIndex = 0; columnIndex < static_cast<int>(config->Columns->size()); ++columnIndex) {
            idToIndexInRow[schema.GetColumnIndexOrThrow((*config->Columns)[columnIndex])] = columnIndex;
        }
    } else {
        for (int id = 0; id < static_cast<int>(schema.Columns().size()); ++id) {
            idToIndexInRow[id] = id;
        }
    }

    return New<TSchemafulWriterForSchemafulDsv>(
        stream, 
        config,
        idToIndexInRow);
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NFormats
} // namespace NYT

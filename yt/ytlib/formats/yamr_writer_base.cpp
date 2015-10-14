#include "stdafx.h"
#include "yamr_writer.h"

#include <ytlib/table_client/name_table.h>

#include <core/misc/error.h>

#include <core/yson/format.h>

namespace NYT {
namespace NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TSchemalessYamrWriterBase::TSchemalessYamrWriterBase(
    TNameTablePtr nameTable, 
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    bool enableKeySwitch,
    int keyColumnCount,
    TYamrFormatConfigBasePtr config)
    : TSchemalessFormatWriterBase(
        nameTable, 
        std::move(output),
        enableContextSaving, 
        enableKeySwitch,
        keyColumnCount)
    , Config_(config)
    , Table_(
        config->FieldSeparator,
        config->RecordSeparator,
        config->EnableEscaping, // Enable key escaping
        config->EnableEscaping, // Enable value escaping
        config->EscapingSymbol,
        true) {
}

void TSchemalessYamrWriterBase::EscapeAndWrite(const TStringBuf& value, bool inKey)
{
    auto* stream = GetOutputStream();
    if (Config_->EnableEscaping) {
        WriteEscaped(
            stream,
            value,
            inKey ? Table_.KeyStops : Table_.ValueStops,
            Table_.Escapes,
            Config_->EscapingSymbol);
    } else {
        stream->Write(value);
    }
}

void TSchemalessYamrWriterBase::WriteInLenvalMode(const TStringBuf& value)
{
    auto* stream = GetOutputStream();
    WritePod(*stream, static_cast<ui32>(value.size()));
    stream->Write(value);
}

void TSchemalessYamrWriterBase::WriteTableIndex(int tableIndex)
{
    auto* stream = GetOutputStream();
    
    if (!Config_->EnableTableIndex) {
        // Silently ignore table switches.
        return;
    }

    if (Config_->Lenval) {
        WritePod(*stream, static_cast<ui32>(-1));
        WritePod(*stream, static_cast<ui32>(tableIndex));
    } else {
        stream->Write(ToString(tableIndex));
        stream->Write(Config_->RecordSeparator);
    }
}

void TSchemalessYamrWriterBase::WriteRangeIndex(i32 rangeIndex)
{
    auto* stream = GetOutputStream();

    if (!Config_->Lenval) {
        THROW_ERROR_EXCEPTION("Range indices are not supported in text YAMR format");
    }
    WritePod(*stream, static_cast<ui32>(-3));
    WritePod(*stream, static_cast<ui32>(rangeIndex));
}

void TSchemalessYamrWriterBase::WriteRowIndex(i64 rowIndex)
{
    auto* stream = GetOutputStream();

    if (!Config_->Lenval) {
         THROW_ERROR_EXCEPTION("Row indices are not supported in text YAMR format");
    }
    WritePod(*stream, static_cast<ui32>(-4));
    WritePod(*stream, static_cast<ui64>(rowIndex));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

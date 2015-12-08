#include "yamr_writer_base.h"
#include "yamr_writer.h"

#include <yt/ytlib/table_client/name_table.h>

#include <yt/core/misc/error.h>

#include <yt/core/yson/format.h>

namespace NYT {
namespace NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TSchemalessWriterForYamrBase::TSchemalessWriterForYamrBase(
    TNameTablePtr nameTable, 
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
<<<<<<< HEAD
    TControlAttributesConfigPtr controlAttributesConfig,
=======
    bool enableKeySwitch,
>>>>>>> origin/prestable/0.17.4
    int keyColumnCount,
    TYamrFormatConfigBasePtr config)
    : TSchemalessFormatWriterBase(
        nameTable, 
        std::move(output),
        enableContextSaving, 
<<<<<<< HEAD
        controlAttributesConfig,
        keyColumnCount)
    , Config_(config)
{ }

void TSchemalessWriterForYamrBase::EscapeAndWrite(
    const TStringBuf& value, 
    TLookupTable stops, 
    TEscapeTable escapes)
=======
        enableKeySwitch,
        keyColumnCount)
    , Config_(config)
{
}

void TSchemalessWriterForYamrBase::EscapeAndWrite(const TStringBuf& value, TLookupTable stops, TEscapeTable escapes)
>>>>>>> origin/prestable/0.17.4
{
    auto* stream = GetOutputStream();
    if (Config_->EnableEscaping) {
        WriteEscaped(
            stream,
            value,
            stops,
            escapes,
            Config_->EscapingSymbol);
    } else {
        stream->Write(value);
    }
}

void TSchemalessWriterForYamrBase::WriteInLenvalMode(const TStringBuf& value)
{
    auto* stream = GetOutputStream();
    WritePod(*stream, static_cast<ui32>(value.size()));
    stream->Write(value);
}

<<<<<<< HEAD
void TSchemalessWriterForYamrBase::WriteTableIndex(i64 tableIndex)
=======
void TSchemalessWriterForYamrBase::WriteTableIndex(i32 tableIndex)
>>>>>>> origin/prestable/0.17.4
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

<<<<<<< HEAD
void TSchemalessWriterForYamrBase::WriteRangeIndex(i64 rangeIndex)
{
    YCHECK(Config_->Lenval);

    auto* stream = GetOutputStream();
=======
void TSchemalessWriterForYamrBase::WriteRangeIndex(i32 rangeIndex)
{
    auto* stream = GetOutputStream();

    if (!Config_->Lenval) {
        THROW_ERROR_EXCEPTION("Range indices are not supported in text YAMR format");
    }
>>>>>>> origin/prestable/0.17.4
    WritePod(*stream, static_cast<ui32>(-3));
    WritePod(*stream, static_cast<ui32>(rangeIndex));
}

void TSchemalessWriterForYamrBase::WriteRowIndex(i64 rowIndex)
{
<<<<<<< HEAD
    YCHECK(Config_->Lenval);

    auto* stream = GetOutputStream();
=======
    auto* stream = GetOutputStream();

    if (!Config_->Lenval) {
         THROW_ERROR_EXCEPTION("Row indices are not supported in text YAMR format");
    }
>>>>>>> origin/prestable/0.17.4
    WritePod(*stream, static_cast<ui32>(-4));
    WritePod(*stream, static_cast<ui64>(rowIndex));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

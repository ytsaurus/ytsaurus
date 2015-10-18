#pragma once

#include "public.h"
#include "config.h"
#include "helpers.h"
#include "yamr_table.h"
#include "schemaless_writer_adapter.h"

#include <ytlib/table_client/public.h>

#include <core/misc/blob_output.h>
#include <core/misc/nullable.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TSchemalessYamrWriterBase
    : public TSchemalessFormatWriterBase
{
public:
    TSchemalessYamrWriterBase(
        NTableClient::TNameTablePtr nameTable,
        NConcurrency::IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        bool enableKeySwitch,
        int keyColumnCount,
        TYamrFormatConfigBasePtr config);

    // ISchemalessFormatWriter overrides.
    virtual void WriteTableIndex(int tableIndex) override;
    virtual void WriteRangeIndex(i32 rangeIndex) override;
    virtual void WriteRowIndex(i64 rowIndex) override;

protected:
    TYamrFormatConfigBasePtr Config_;

    void WriteInLenvalMode(const TStringBuf& value);
    
    void EscapeAndWrite(const TStringBuf& value, TLookupTable stops, TEscapeTable escapes);
};

DEFINE_REFCOUNTED_TYPE(TSchemalessYamrWriterBase)

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

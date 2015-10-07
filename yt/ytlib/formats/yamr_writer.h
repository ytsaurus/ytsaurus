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

//! Note: only tabular format is supported.
class TSchemalessYamrWriter
    : public TSchemalessFormatWriterBase {
public:
    TSchemalessYamrWriter(
        NTableClient::TNameTablePtr nameTable,
        NConcurrency::IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        bool enableKeySwitch,
        int keyColumnCount,
        TYamrFormatConfigPtr config = New<TYamrFormatConfig>());

    // ISchemalessFormatWriter overrides.
    virtual void DoWrite(const std::vector<NTableClient::TUnversionedRow>& rows) override;
    virtual void WriteTableIndex(int tableIndex) override;
    virtual void WriteRangeIndex(i32 rangeIndex) override;
    virtual void WriteRowIndex(i64 rowIndex) override;

private:
    TYamrFormatConfigPtr Config_;
    TYamrTable Table_;

    i32 KeyId_, SubKeyId_, ValueId_;

    void WriteInLenvalMode(const TStringBuf& value);
    
    void EscapeAndWrite(const TStringBuf& value, bool inKey);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

#pragma once

#include "public.h"
#include "config.h"
#include "helpers.h"
#include "escape.h"
#include "schemaless_writer_adapter.h"

#include <yt/client/table_client/public.h>

#include <yt/core/misc/blob_output.h>
#include <yt/core/misc/nullable.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterForYamrBase
    : public TSchemalessFormatWriterBase
{
public:
    TSchemalessWriterForYamrBase(
        NTableClient::TNameTablePtr nameTable,
        NConcurrency::IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        TControlAttributesConfigPtr controlAttributesConfig,
        int keyColumnCount,
        TYamrFormatConfigBasePtr config);

protected:
    TYamrFormatConfigBasePtr Config_;
    bool TableIndexWasWritten_ = false;
    int CurrentTableIndex_ = 0;

    void WriteInLenvalMode(TStringBuf value);

    virtual void WriteTableIndex(i64 tableIndex) override;
    virtual void WriteRangeIndex(i64 rangeIndex) override;
    virtual void WriteRowIndex(i64 rowIndex) override;
};

DEFINE_REFCOUNTED_TYPE(TSchemalessWriterForYamrBase)

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

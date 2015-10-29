#pragma once

#include "public.h"
#include "config.h"
#include "helpers.h"
#include "yamr_table.h"
#include "yamr_writer_base.h"

#include <ytlib/table_client/public.h>

#include <core/misc/blob_output.h>
#include <core/misc/nullable.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterForYamr
    : public TSchemalessWriterForYamrBase
{
public:
    TSchemalessWriterForYamr(
        NTableClient::TNameTablePtr nameTable,
        NConcurrency::IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        bool enableKeySwitch,
        int keyColumnCount,
        TYamrFormatConfigPtr config = New<TYamrFormatConfig>());

    // ISchemalessFormatWriter override.
    virtual void DoWrite(const std::vector<NTableClient::TUnversionedRow>& rows) override;

private:
    int KeyId_;
    int SubkeyId_;
    int ValueId_;
    
    TYamrTable Table_;

    void ValidateColumnType(const NTableClient::TUnversionedValue* value);
};

DEFINE_REFCOUNTED_TYPE(TSchemalessWriterForYamr)

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

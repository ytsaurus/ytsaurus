#pragma once

#include "public.h"
#include "config.h"
#include "dsv_table.h"
#include "helpers.h"
#include "yamr_writer_base.h"

#include <yt/ytlib/table_client/public.h>

#include <yt/core/misc/blob_output.h>
#include <yt/core/misc/small_set.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterForYamredDsv
    : public TSchemalessWriterForYamrBase
{
public:
    TSchemalessWriterForYamredDsv(
        NTableClient::TNameTablePtr nameTable,
        NConcurrency::IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        TControlAttributesConfigPtr controlAttributesConfig,
        int keyColumnCount,
        TYamredDsvFormatConfigPtr config = New<TYamredDsvFormatConfig>());

    // ISchemalessFormatWriter override.
    virtual void DoWrite(const std::vector<NTableClient::TUnversionedRow>& rows) override;
private:
    std::vector<TNullable<TStringBuf>> RowValues_;
    
    std::vector<int> KeyColumnIds_;
    std::vector<int> SubkeyColumnIds_;
    std::vector<Stroka> EscapedColumnNames_;

    TDsvTable Table_;

    void WriteYamrKey(const std::vector<int>& columnIds);
    ui32 CalculateTotalKeyLength(const std::vector<int>& columnIds);
    void WriteYamrValue(); 
    ui32 CalculateTotalValueLength();
    ui32 CalculateLength(const TStringBuf& string, bool inKey);

    void UpdateEscapedColumnNames();
};

DEFINE_REFCOUNTED_TYPE(TSchemalessWriterForYamredDsv)

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT


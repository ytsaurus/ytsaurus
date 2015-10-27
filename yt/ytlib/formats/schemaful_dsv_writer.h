#pragma once

#include "public.h"
#include "config.h"
#include "helpers.h"
#include "schemaful_dsv_table.h"
#include "schemaless_writer_adapter.h"

#include <core/misc/blob.h>
#include <core/misc/nullable.h>

#include <core/concurrency/async_stream.h>

#include <ytlib/table_client/public.h>
#include <ytlib/table_client/schemaful_writer.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

// This class contains methods common for TSchemafulDsvWriter and TSchemalessWriterForSchemafulDsv.
class TSchemafulDsvWriterBase
{
protected:
    TBlob Buffer_;
    
    TSchemafulDsvFormatConfigPtr Config_;

    TSchemafulDsvWriterBase(TSchemafulDsvFormatConfigPtr config);
    
    std::vector<int> ColumnIdMapping_;

    void WriteValue(const NTableClient::TUnversionedValue& value);
    
    void WriteRaw(const TStringBuf& str);
    void WriteRaw(char ch);
    
private:
    static char* WriteInt64Reversed(char* ptr, i64 value);
    static char* WriteUint64Reversed(char* ptr, ui64 value);    
};

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterForSchemafulDsv
    : public TSchemalessFormatWriterBase
    , public TSchemafulDsvWriterBase
{
public:
    TSchemalessWriterForSchemafulDsv(
        NTableClient::TNameTablePtr nameTable,
        NConcurrency::IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        TSchemafulDsvFormatConfigPtr config);
       
    // ISchemalessFormatWriter overrides.
    virtual void DoWrite(const std::vector<NTableClient::TUnversionedRow>& rows) override;
    virtual void WriteTableIndex(i32 tableIndex) override;
    virtual void WriteRangeIndex(i32 rangeIndex) override;
    virtual void WriteRowIndex(i64 rowIndex) override;
private:
    std::vector<int> IdToIndexInRowMapping_;
};

DEFINE_REFCOUNTED_TYPE(TSchemalessWriterForSchemafulDsv)

////////////////////////////////////////////////////////////////////////////////

class TSchemafulDsvWriter
    : public NTableClient::ISchemafulWriter
    , public TSchemafulDsvWriterBase
{
public:
    TSchemafulDsvWriter(
        NConcurrency::IAsyncOutputStreamPtr stream,
        std::vector<int> columnIdMapping,
        TSchemafulDsvFormatConfigPtr config = New<TSchemafulDsvFormatConfig>());

    virtual TFuture<void> Close() override;

    virtual bool Write(const std::vector<NTableClient::TUnversionedRow>& rows) override;

    virtual TFuture<void> GetReadyEvent() override;

private:
    NConcurrency::IAsyncOutputStreamPtr Stream_;

    TFuture<void> Result_;
};

////////////////////////////////////////////////////////////////////////////////

NTableClient::ISchemafulWriterPtr CreateSchemafulDsvWriter(
    NConcurrency::IAsyncOutputStreamPtr stream,
    const NTableClient::TTableSchema& schema,
    TSchemafulDsvFormatConfigPtr config = New<TSchemafulDsvFormatConfig>());

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT


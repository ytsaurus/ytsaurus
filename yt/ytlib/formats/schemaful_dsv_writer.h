#pragma once

#include "public.h"
#include "config.h"
#include "helpers.h"
#include "schemaful_dsv_table.h"
#include "schemaless_writer_adapter.h"

#include <core/misc/blob.h>
#include <core/misc/blob_output.h>
#include <core/misc/nullable.h>

#include <core/concurrency/async_stream.h>

#include <ytlib/table_client/public.h>
#include <ytlib/table_client/schemaful_writer.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

// This class contains methods common for TSchemafulWriterForSchemafulDsv and TSchemalessWriterForSchemafulDsv.
class TSchemafulDsvWriterBase
{
protected:
    TBlobOutput* BlobOutput_;
    
    TSchemafulDsvFormatConfigPtr Config_;

    TSchemafulDsvWriterBase(TSchemafulDsvFormatConfigPtr config);
    
    std::vector<int> ColumnIdMapping_;

    void WriteValue(const NTableClient::TUnversionedValue& value);
    
    void WriteRaw(const TStringBuf& str);
    void WriteRaw(char ch);

    void EscapeAndWrite(const TStringBuf& string);
private:
    TSchemafulDsvTable Table_;
    
    static char* WriteInt64Backwards(char* ptr, i64 value);
    static char* WriteUint64Backwards(char* ptr, ui64 value);    
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

class TSchemafulWriterForSchemafulDsv
    : public NTableClient::ISchemafulWriter
    , public TSchemafulDsvWriterBase
{
public:
    TSchemafulWriterForSchemafulDsv(
        NConcurrency::IAsyncOutputStreamPtr stream,
        std::vector<int> columnIdMapping,
        TSchemafulDsvFormatConfigPtr config = New<TSchemafulDsvFormatConfig>());

    virtual TFuture<void> Close() override;

    virtual bool Write(const std::vector<NTableClient::TUnversionedRow>& rows) override;

    virtual TFuture<void> GetReadyEvent() override;

private:
    std::unique_ptr<TOutputStream> Output_;

    void TryFlushBuffer(bool force);
    void DoFlushBuffer();

    TFuture<void> Result_;

    TBlobOutput UnderlyingBlobOutput_;
};

////////////////////////////////////////////////////////////////////////////////

NTableClient::ISchemafulWriterPtr CreateSchemafulWriterForSchemafulDsv(
    NConcurrency::IAsyncOutputStreamPtr stream,
    const NTableClient::TTableSchema& schema,
    TSchemafulDsvFormatConfigPtr config = New<TSchemafulDsvFormatConfig>());

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT


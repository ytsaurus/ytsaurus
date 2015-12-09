#pragma once

#include "public.h"
#include "config.h"
#include "helpers.h"
#include "schemaful_dsv_table.h"
#include "schemaless_writer_adapter.h"

#include <yt/ytlib/table_client/schemaful_writer.h>

#include <yt/core/concurrency/async_stream.h>

#include <yt/core/misc/blob.h>
#include <yt/core/misc/blob_output.h>
#include <yt/core/misc/nullable.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

// This class contains methods common for TSchemafulWriterForSchemafulDsv and TSchemalessWriterForSchemafulDsv.
class TSchemafulDsvWriterBase
{
protected:
    TBlobOutput* BlobOutput_;
    
    TSchemafulDsvFormatConfigPtr Config_;
    
    std::vector<int> ColumnIdMapping_;
    
    TSchemafulDsvWriterBase(TSchemafulDsvFormatConfigPtr config);

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
        TControlAttributesConfigPtr controlAttributesConfig,
        TSchemafulDsvFormatConfigPtr config);
       
    // ISchemalessFormatWriter overrides.
    virtual void DoWrite(const std::vector<NTableClient::TUnversionedRow>& rows) override;
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

    void TryFlushBuffer();
    void DoFlushBuffer(bool force);
};

////////////////////////////////////////////////////////////////////////////////

NTableClient::ISchemafulWriterPtr CreateSchemafulWriterForSchemafulDsv(
    NConcurrency::IAsyncOutputStreamPtr stream,
    const NTableClient::TTableSchema& schema,
    TSchemafulDsvFormatConfigPtr config = New<TSchemafulDsvFormatConfig>());

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT


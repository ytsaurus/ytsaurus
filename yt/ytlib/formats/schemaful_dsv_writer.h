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

<<<<<<< HEAD
ISchemalessFormatWriterPtr CreateSchemalessWriterForSchemafulDsv(
    TSchemafulDsvFormatConfigPtr config,
    NTableClient::TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount);

ISchemalessFormatWriterPtr CreateSchemalessWriterForSchemafulDsv(
    const NYTree::IAttributeDictionary& attributes,
    NTableClient::TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount);

////////////////////////////////////////////////////////////////////////////////

NTableClient::ISchemafulWriterPtr CreateSchemafulWriterForSchemafulDsv(
    TSchemafulDsvFormatConfigPtr config,
    const NTableClient::TTableSchema& schema,
    NConcurrency::IAsyncOutputStreamPtr stream);
=======
// This class contains methods common for TSchemafulWriterForSchemafulDsv and TSchemalessWriterForSchemafulDsv.
class TSchemafulDsvWriterBase
{
protected:
    TBlobOutput* BlobOutput_;
    
    TSchemafulDsvFormatConfigPtr Config_;
    
    // This array indicates on which position should each
    // column stay in the resulting row.
    std::vector<int> IdToIndexInRow_;
   
    // This array contains TUnversionedValue's reordered
    // according to the desired order.
    std::vector<const NTableClient::TUnversionedValue*> CurrentRowValues_;
    
    TSchemafulDsvWriterBase(
        TSchemafulDsvFormatConfigPtr config,
        std::vector<int> idToIndexInRow);

    void WriteValue(const NTableClient::TUnversionedValue& value);
    
    void WriteRaw(const TStringBuf& str);
    void WriteRaw(char ch);

    void WriteInt64(i64 value);
    void WriteUint64(ui64 value);

    void EscapeAndWrite(const TStringBuf& string);
    
    int FindMissingValueIndex() const; 
private:
    TSchemafulDsvTable Table_;
 
    static char* WriteInt64ToBufferBackwards(char* ptr, i64 value);
    static char* WriteUint64ToBufferBackwards(char* ptr, ui64 value);    
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
        TSchemafulDsvFormatConfigPtr config,
        std::vector<int> idToIndexInRow);
       
    // ISchemalessFormatWriter overrides.
    virtual void DoWrite(const std::vector<NTableClient::TUnversionedRow>& rows) override;
    virtual void WriteTableIndex(i32 tableIndex) override;
    virtual void WriteRangeIndex(i32 rangeIndex) override;
    virtual void WriteRowIndex(i64 rowIndex) override;
private:
    std::vector<int> IdToIndexInRowMapping_;
    i32 CurrentTableIndex_ = -1;
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
        TSchemafulDsvFormatConfigPtr config,
        std::vector<int> idToIndexInRow);

    virtual TFuture<void> Close() override;

    virtual bool Write(const std::vector<NTableClient::TUnversionedRow>& rows) override;

    virtual TFuture<void> GetReadyEvent() override;

private:
    std::unique_ptr<TOutputStream> Output_;

    TFuture<void> Result_;

    TBlobOutput UnderlyingBlobOutput_;

    void TryFlushBuffer(bool force);
    void DoFlushBuffer();
};

////////////////////////////////////////////////////////////////////////////////
>>>>>>> origin/prestable/0.17.5

NTableClient::ISchemafulWriterPtr CreateSchemafulWriterForSchemafulDsv(
    const NYTree::IAttributeDictionary& attributes,
    const NTableClient::TTableSchema& schema,
    NConcurrency::IAsyncOutputStreamPtr stream);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT


#pragma once 

#include "public.h"
#include "format.h"
#include "helpers.h"

#include <ytlib/table_client/public.h>
#include <ytlib/table_client/schemaless_writer.h>

#include <core/yson/public.h>

#include <core/concurrency/public.h>

#include <core/misc/blob_output.h>

#include <memory>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TSchemalessFormatWriterBase
    : public ISchemalessFormatWriter
{
public:
    virtual TFuture<void> Open() override;

    virtual bool Write(const std::vector<NTableClient::TUnversionedRow> &rows) override;

    virtual TFuture<void> GetReadyEvent() override;

    virtual TFuture<void> Close() override;

    virtual NTableClient::TNameTablePtr GetNameTable() const override;

    virtual bool IsSorted() const override;

    virtual TBlob GetContext() const;

protected:
    TSchemalessFormatWriterBase(
        NTableClient::TNameTablePtr nameTable,
        bool enableContextSaving,
        NConcurrency::IAsyncOutputStreamPtr output);

    TOutputStream* GetOutputStream();

    void TryFlushBuffer();

    virtual void DoWrite(const std::vector<NTableClient::TUnversionedRow> &rows) = 0;

private:
    bool EnableContextSaving_;
    TBlobOutput CurrentBuffer_;
    TBlobOutput PreviousBuffer_;
    std::unique_ptr<TOutputStream> Output_;

    TError Error_;
    NTableClient::TNameTablePtr NameTable_;

    void DoFlushBuffer(bool force);
};

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterAdapter
    : public TSchemalessFormatWriterBase
{
public:
    TSchemalessWriterAdapter(
        const TFormat& format,
        NTableClient::TNameTablePtr nameTable,
        NConcurrency::IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        bool enableKeySwitch,
        int keyColumnCount);

    virtual void WriteTableIndex(int tableIndex) override;

    virtual void WriteRangeIndex(i32 rangeIndex) override;

    virtual void WriteRowIndex(i64 rowIndex) override;

private:
    std::unique_ptr<NYson::IYsonConsumer> Consumer_;
    NTableClient::TNameTablePtr NameTable_;

    bool EnableKeySwitch_;
    NTableClient::TOwningKey LastKey_;
    NTableClient::TKey CurrentKey_;

    int KeyColumnCount_;
    TError Error_;

    template <class T>
    void WriteControlAttribute(
        NTableClient::EControlAttribute controlAttribute,
        T value);

    void ConsumeRow(const NTableClient::TUnversionedRow& row);

    virtual void DoWrite(const std::vector<NTableClient::TUnversionedRow> &rows) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

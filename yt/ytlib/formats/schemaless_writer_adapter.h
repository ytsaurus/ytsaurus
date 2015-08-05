#pragma once 

#include "public.h"
#include "format.h"

#include <ytlib/table_client/public.h>
#include <ytlib/table_client/schemaless_writer.h>

#include <core/yson/public.h>

#include <core/concurrency/public.h>

#include <core/misc/blob_output.h>

#include <memory>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterAdapter
    : public ISchemalessFormatWriter
{
public:
    TSchemalessWriterAdapter(
        const TFormat& format,
        NTableClient::TNameTablePtr nameTable,
        NConcurrency::IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        bool enableKeySwitch,
        int keyColumnCount);

    virtual TFuture<void> Open() override;

    virtual bool Write(const std::vector<NTableClient::TUnversionedRow> &rows) override;

    virtual TFuture<void> GetReadyEvent() override;

    virtual TFuture<void> Close() override;

    virtual NTableClient::TNameTablePtr GetNameTable() const override;

    virtual bool IsSorted() const override;

    virtual void WriteTableIndex(int tableIndex) override;

    virtual void WriteRangeIndex(i32 rangeIndex) override;

    virtual void WriteRowIndex(i64 rowIndex) override;

    virtual TBlob GetContext() const override;

private:
    std::unique_ptr<NYson::IYsonConsumer> Consumer_;
    NTableClient::TNameTablePtr NameTable_;

    std::unique_ptr<TOutputStream> Output_;

    bool EnableContextSaving_;
    TBlobOutput CurrentBuffer_;
    TBlobOutput PreviousBuffer_;

    bool EnableKeySwitch_;
    NTableClient::TOwningKey LastKey_;
    NTableClient::TKey CurrentKey_;

    int KeyColumnCount_;

    static TFuture<void> StaticError_;
    TError Error_;

    template <class T>
    void WriteControlAttribute(
        NTableClient::EControlAttribute controlAttribute,
        T value);

    void ConsumeRow(const NTableClient::TUnversionedRow& row);
    void FlushBuffer();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

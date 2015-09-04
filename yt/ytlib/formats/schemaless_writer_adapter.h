#pragma once 

#include "public.h"
#include "format.h"
#include "helpers.h"

#include <ytlib/new_table_client/public.h>
#include <ytlib/new_table_client/schemaless_writer.h>

#include <core/yson/public.h>

#include <core/concurrency/public.h>

#include <memory>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterAdapter
    : public ISchemalessFormatWriter
    , public TContextSavingMixin
{
public:
    TSchemalessWriterAdapter(
        const TFormat& format,
        NVersionedTableClient::TNameTablePtr nameTable,
        NConcurrency::IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        bool enableKeySwitch,
        int keyColumnCount);

    virtual TFuture<void> Open() override;

    virtual bool Write(const std::vector<NVersionedTableClient::TUnversionedRow> &rows) override;

    virtual TFuture<void> GetReadyEvent() override;

    virtual TFuture<void> Close() override;

    virtual NVersionedTableClient::TNameTablePtr GetNameTable() const override;

    virtual bool IsSorted() const override;

    virtual void WriteTableIndex(int tableIndex) override;

    virtual void WriteRangeIndex(i32 rangeIndex) override;

    virtual void WriteRowIndex(i64 rowIndex) override;

    virtual TBlob GetContext() const override;

private:
    std::unique_ptr<NYson::IYsonConsumer> Consumer_;
    NVersionedTableClient::TNameTablePtr NameTable_;

    bool EnableKeySwitch_;
    NVersionedTableClient::TOwningKey LastKey_;
    NVersionedTableClient::TKey CurrentKey_;

    int KeyColumnCount_;

    static TFuture<void> StaticError_;
    TError Error_;

    template <class T>
    void WriteControlAttribute(
        NVersionedTableClient::EControlAttribute controlAttribute,
        T value);

    void ConsumeRow(const NVersionedTableClient::TUnversionedRow& row);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

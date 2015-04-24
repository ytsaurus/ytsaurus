#pragma once 

#include "public.h"
#include "format.h"

#include <ytlib/new_table_client/public.h>
#include <ytlib/new_table_client/schemaless_writer.h>

#include <core/yson/public.h>

#include <core/misc/blob_output.h>

#include <memory>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterAdapter
    : public NVersionedTableClient::ISchemalessFormatWriter
{
public:
    TSchemalessWriterAdapter(
        const TFormat& format,
        NVersionedTableClient::TNameTablePtr nameTable,
        std::unique_ptr<TOutputStream> outputStream,
        bool enableTableSwitch,
        bool enableKeySwitch,
        int keyColumnCount);

    virtual TFuture<void> Open() override;

    virtual bool Write(const std::vector<NVersionedTableClient::TUnversionedRow> &rows) override;

    virtual TFuture<void> GetReadyEvent() override;

    virtual TFuture<void> Close() override;

    virtual NVersionedTableClient::TNameTablePtr GetNameTable() const override;

    virtual bool IsSorted() const override;

    virtual void SetTableIndex(int tableIndex) override;

    virtual TBlob GetContext() const override;

private:
    std::unique_ptr<NYson::IYsonConsumer> Consumer_;
    NVersionedTableClient::TNameTablePtr NameTable_;

    std::unique_ptr<TOutputStream> OutputStream_;
    TBlobOutput CurrentBuffer_;
    TBlobOutput PreviousBuffer_;

    bool EnableTableSwitch_;
    int TableIndex_ = -1;

    bool EnableKeySwitch_;
    int KeyColumnCount_;
    NVersionedTableClient::TOwningKey LastKey_;
    NVersionedTableClient::TKey CurrentKey_;

    static TFuture<void> StaticError_;
    TError Error_;

    void ConsumeRow(const NVersionedTableClient::TUnversionedRow& row);
    void FlushBuffer();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

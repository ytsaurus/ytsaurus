#pragma once 

#include "public.h"

#include <ytlib/new_table_client/schemaless_writer.h>

#include <core/yson/public.h>

#include <core/misc/nullable.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterAdapter
    : public NVersionedTableClient::ISchemalessMultiSourceWriter
{
public:
    TSchemalessWriterAdapter(
        std::unique_ptr<NYson::IYsonConsumer> consumer,
        NVersionedTableClient::TNameTablePtr nameTable,
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

private:
    std::unique_ptr<NYson::IYsonConsumer> Consumer_;
    NVersionedTableClient::TNameTablePtr NameTable_;

    bool EnableTableSwitch_;
    int TableIndex_ = -1;

    bool EnableKeySwitch_;
    int KeyColumnCount_;
    NVersionedTableClient::TOwningKey LastKey_;
    NVersionedTableClient::TKey CurrentKey_;

    static TFuture<void> StaticError_;
    TError Error_;

    void ConsumeRow(const NVersionedTableClient::TUnversionedRow& row);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

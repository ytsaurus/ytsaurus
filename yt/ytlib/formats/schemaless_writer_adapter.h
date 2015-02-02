#pragma once 

#include "public.h"

#include <ytlib/new_table_client/schemaless_writer.h>

#include <core/yson/public.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterAdapter
    : public NVersionedTableClient::ISchemalessWriter
{
public:
    TSchemalessWriterAdapter(
        std::unique_ptr<NYson::IYsonConsumer> consumer,
        NVersionedTableClient::TNameTablePtr nameTable);

    virtual TFuture<void> Open() override;

    virtual bool Write(const std::vector<NVersionedTableClient::TUnversionedRow> &rows) override;

    virtual TFuture<void> GetReadyEvent() override;

    virtual TFuture<void> Close() override;

private:
    std::unique_ptr<NYson::IYsonConsumer> Consumer_;
    NVersionedTableClient::TNameTablePtr NameTable_;

    static TFuture<void> StaticError_;
    TError Error_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

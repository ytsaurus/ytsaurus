#pragma once

#include "public.h"

#include <core/misc/blob_output.h>

#include <core/concurrency/public.h>

#include <core/actions/future.h>

#include <core/yson/writer.h>

#include <ytlib/new_table_client/schemaful_writer.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TSchemafulYsonWriter
    : public NVersionedTableClient::ISchemafulWriter
{
public:
    explicit TSchemafulYsonWriter(
        NConcurrency::IAsyncOutputStreamPtr stream,
        TYsonFormatConfigPtr config = New<TYsonFormatConfig>());

    virtual TAsyncError Open(
        const NVersionedTableClient::TTableSchema& schema,
        const TNullable<NVersionedTableClient::TKeyColumns>& keyColumns) override;

    virtual TAsyncError Close() override;

    virtual bool Write(const std::vector<NVersionedTableClient::TUnversionedRow>& rows) override;

    virtual TAsyncError GetReadyEvent() override;

private:
    NConcurrency::IAsyncOutputStreamPtr Stream_;

    TBlobOutput Buffer_;
    NYson::TYsonWriter Writer_;

    NVersionedTableClient::TTableSchema Schema_;

    TAsyncError Result_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT


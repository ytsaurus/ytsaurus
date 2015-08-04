#pragma once

#include "public.h"

#include <core/misc/blob_output.h>

#include <core/concurrency/public.h>

#include <core/actions/future.h>

#include <core/yson/writer.h>

#include <ytlib/table_client/schemaful_writer.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TSchemafulYsonWriter
    : public NTableClient::ISchemafulWriter
{
public:
    explicit TSchemafulYsonWriter(
        NConcurrency::IAsyncOutputStreamPtr stream,
        TYsonFormatConfigPtr config = New<TYsonFormatConfig>());

    virtual TFuture<void> Open(
        const NTableClient::TTableSchema& schema,
        const TNullable<NTableClient::TKeyColumns>& keyColumns) override;

    virtual TFuture<void> Close() override;

    virtual bool Write(const std::vector<NTableClient::TUnversionedRow>& rows) override;

    virtual TFuture<void> GetReadyEvent() override;

private:
    NConcurrency::IAsyncOutputStreamPtr Stream_;

    TYsonFormatConfigPtr Config_;
    TBlobOutput Buffer_;
    NYson::TYsonWriter Writer_;

    NTableClient::TTableSchema Schema_;

    TFuture<void> Result_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT


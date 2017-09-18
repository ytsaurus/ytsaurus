#pragma once

#include "public.h"

#include <yt/ytlib/table_client/schemaful_writer.h>

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/misc/blob_output.h>

#include <yt/core/yson/writer.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TSchemafulWriter
    : public NTableClient::ISchemafulWriter
{
public:
    TSchemafulWriter(
        NConcurrency::IAsyncOutputStreamPtr stream,
        const NTableClient::TTableSchema& schema,
        const std::function<std::unique_ptr<NYson::IFlushableYsonConsumer>(IOutputStream*)>& consumerBuilder);

    virtual TFuture<void> Close() override;

    virtual bool Write(const TRange<NTableClient::TUnversionedRow>& rows) override;

    virtual TFuture<void> GetReadyEvent() override;

private:
    const NConcurrency::IAsyncOutputStreamPtr Stream_;
    const NTableClient::TTableSchema Schema_;
    const std::unique_ptr<NYson::IFlushableYsonConsumer> Consumer_;

    TBlobOutput Buffer_;

    TFuture<void> Result_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT


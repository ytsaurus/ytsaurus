#pragma once

#include "public.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_writer.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/blob_output.h>

#include <yt/yt/core/yson/writer.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

class TSchemafulWriter
    : public NTableClient::IUnversionedRowsetWriter
{
public:
    TSchemafulWriter(
        NConcurrency::IAsyncOutputStreamPtr stream,
        NTableClient::TTableSchemaPtr schema,
        const std::function<std::unique_ptr<NYson::IFlushableYsonConsumer>(IOutputStream*)>& consumerBuilder);

    virtual TFuture<void> Close() override;

    virtual bool Write(TRange<NTableClient::TUnversionedRow> rows) override;

    virtual TFuture<void> GetReadyEvent() override;

private:
    const NConcurrency::IAsyncOutputStreamPtr Stream_;
    const NTableClient::TTableSchemaPtr Schema_;
    const std::unique_ptr<NYson::IFlushableYsonConsumer> Consumer_;

    TBlobOutput Buffer_;

    TFuture<void> Result_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats


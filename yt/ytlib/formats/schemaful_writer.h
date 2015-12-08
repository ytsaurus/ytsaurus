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
    template <class TMakeWriter>
    explicit TSchemafulWriter(
        NConcurrency::IAsyncOutputStreamPtr stream,
        const NTableClient::TTableSchema& schema,
        TMakeWriter&& makeWriter)
        : Stream_(stream)
        , Schema_(schema)
        , Writer_(makeWriter(&Buffer_))
    { }

    virtual TFuture<void> Close() override;

    virtual bool Write(const std::vector<NTableClient::TUnversionedRow>& rows) override;

    virtual TFuture<void> GetReadyEvent() override;

private:
    NConcurrency::IAsyncOutputStreamPtr Stream_;

    NTableClient::TTableSchema Schema_;

    TBlobOutput Buffer_;
    std::unique_ptr<NYson::TYsonConsumerBase> Writer_;

    TFuture<void> Result_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

